import json
from time import sleep

import helpers.hh_api_requests as hh_api_requests
import helpers.hh_vacancy_parse as hh_vacancy_parse
import helpers.misc_helpers as misc_helpers
from helpers.db_helper import update_and_insert
from helpers.df_files import load_df, move_files_processed, write_list_of_df
from helpers.logger import MyLogger
from postprocess_vacancies import process_vacancies

CONFIG = {
    "COUNTRY_ID": "113",  # Россия
    "FILENAME_LOG": "hhproject",
    "CONSOLE_LOG": True,
    "CONSOLE_LEVEL_LOG": "info",
    "TIMEOUT": 1,
    "VACANCY_DF_SIZE": 100,
    "HH_REQUEST_PERIOD": 8,
    "HH_REQUEST_PER_PAGE": 20,
    "DIR_CSV": "./csv",
    "DIR_CSV_PROCESSED": "./csv/processed",
    "DIR_DICT": "./dict",
    "DB_SCHEMA": "hh_project",
}

TITLE_LIST = [
    "analyst",
    "аналитик",
    "data",
    "дата",
    "dwh",
    "etl",
    "bi",
]

VACANCY_LIST_PARAMS = {
    "text": "",
    "professional_role": "",
    "period": CONFIG["HH_REQUEST_PERIOD"],
    "area": CONFIG["COUNTRY_ID"],
    "page": "",
    "per_page": CONFIG["HH_REQUEST_PER_PAGE"],
    "order_by": "publication_time",
    "responses_count_enabled": True,
    "host": "hh.ru",
    "no_magic": True,
}


logger = MyLogger(
    name=CONFIG["FILENAME_LOG"],
    stream=CONFIG["CONSOLE_LOG"],
    stream_level=CONFIG["CONSOLE_LEVEL_LOG"],
)


role_dict = hh_api_requests.get_professional_roles(TITLE_LIST)
region_dict = hh_api_requests.get_areas_by_country(CONFIG["COUNTRY_ID"])

timestamp = misc_helpers.get_now_filename()

vacancies = []
employers = []
professional_roles = []
key_skills = []
languages = []
industries = []

df_list = {
    "vacancies": {
        "df_list": vacancies,
        "db_table": "vacancies",
        "filename": "".join((timestamp, "_", "vacancies", ".csv")),
        "keys": ["vacancy_id"],
    },
    "employers": {
        "df_list": employers,
        "db_table": "employers",
        "filename": "".join((timestamp, "_", "employers", ".csv")),
        "keys": ["employer_id"],
    },
    "professional_roles": {
        "df_list": professional_roles,
        "db_table": "professional_roles",
        "filename": "".join((timestamp, "_", "professional_roles", ".csv")),
        "keys": ["vacancy_id", "role_id"],
    },
    "key_skills": {
        "df_list": key_skills,
        "db_table": "key_skills",
        "filename": "".join((timestamp, "_", "key_skills", ".csv")),
        "keys": ["vacancy_id", "skill"],
    },
    "industries": {
        "df_list": industries,
        "db_table": "industries",
        "filename": "".join((timestamp, "_", "industries", ".csv")),
        "keys": ["employer_id", "industry_name"],
    },
    "languages": {
        "df_list": languages,
        "db_table": "languages",
        "filename": "".join((timestamp, "_", "languages", ".csv")),
        "keys": ["vacancy_id", "language"],
    },
}

result = {"total": 0, "parsed": 0, "error": 0}

log_message = f'Start scraping API for vacancies in {CONFIG["HH_REQUEST_PERIOD"]} days'
logger.info(log_message)
for title in TITLE_LIST:
    for role_id, role_desc in role_dict.items():
        for region_id, region_name in region_dict.items():
            VACANCY_LIST_PARAMS["page"] = 0
            VACANCY_LIST_PARAMS["text"] = title
            VACANCY_LIST_PARAMS["area"] = region_id
            VACANCY_LIST_PARAMS["professional_role"] = role_id

            pages_limit = hh_vacancy_parse.get_pages(VACANCY_LIST_PARAMS)

            if pages_limit["error"] is None:
                log_message = f'OK | {region_name} | ключевик {title}, роль {role_desc}, вакансий {pages_limit["total"]}, страниц {pages_limit["pages"]}'
                logger.info(log_message)
            else:
                log_message = f'ERROR | {region_name} | ключевик {title}, роль {role_desc}, ошибка {pages_limit["error"]}'
                logger.error(log_message)

            if pages_limit["total"] > 0:
                for page_num in range(0, pages_limit["pages"]):
                    VACANCY_LIST_PARAMS["page"] = page_num
                    response = hh_api_requests.get_vacancies_page(VACANCY_LIST_PARAMS)

                    if response["status"] == "OK":
                        log_message = f"OK | Страница {page_num} из {pages_limit['pages']} | {region_name} | ключевик {title}, роль {role_desc}"
                        logger.info(log_message)
                        data = json.loads(response["data"].decode())
                        for item in data["items"]:
                            vacency_details = hh_vacancy_parse.parse_details(
                                item["url"]
                            )
                            if vacency_details["error"] is None:
                                result["total"] += 1
                                vacency_details["details"]["region_id"] = region_id
                                vacency_details["details"]["region_name"] = region_name
                                vacancies.append(vacency_details["details"])
                                languages.extend(vacency_details["languages"])
                                key_skills.extend(vacency_details["skills"])
                                professional_roles.extend(vacency_details["role"])

                                if item["type"]["id"].lower() != "anonymous":
                                    employer_details = hh_vacancy_parse.parse_employer(
                                        item["employer"]["url"]
                                    )
                                    if employer_details["error"] is None:
                                        log_message = (
                                            f'OK | Страница {item["employer"]["url"]}'
                                        )
                                        logger.debug(log_message)
                                        employers.append(employer_details["details"])
                                        industries.extend(
                                            employer_details["industries"]
                                        )
                                    else:
                                        log_message = f'ERROR | Страница item["employer"]["url"] - {employer_details["error"]}'
                                        logger.error(log_message)

                                log_message = f'OK | Вакансия {item["name"]} - {item["alternate_url"]}'
                                logger.info(log_message)
                                result["parsed"] += 1
                            else:
                                log_message = f'ERROR | Страница item["url"] - {vacency_details["error"]}'
                                logger.error(log_message)
                                result["error"] += 1

                            if len(vacancies) == CONFIG["VACANCY_DF_SIZE"]:
                                write_list_of_df(CONFIG["DIR_CSV"], df_list)
                            sleep(CONFIG["TIMEOUT"])

write_list_of_df(CONFIG["DIR_CSV"], df_list)
log_message = f'Finished scraping API for vacancies. Total vacancies: {result["total"]}, parsed {result["parsed"]}, error {result["error"]}'
logger.info(log_message)


p_vacancies_file = process_vacancies(
    CONFIG["DIR_CSV"], df_list["vacancies"]["filename"]
)
if p_vacancies_file is not None:
    p_vacancies = load_df(CONFIG["DIR_CSV"], p_vacancies_file)
    if p_vacancies["status"] == 0:
        update_and_insert(
            CONFIG["DB_SCHEMA"],
            df_list["vacancies"]["db_table"],
            p_vacancies["df"],
            df_list["vacancies"]["keys"],
        )
        move_files_processed(
            CONFIG["DIR_CSV"],
            CONFIG["DIR_CSV_PROCESSED"],
            df_list["vacancies"]["filename"],
        )
        move_files_processed(
            CONFIG["DIR_CSV"], CONFIG["DIR_CSV_PROCESSED"], p_vacancies_file
        )
    else:
        log_message = 'ERROR | processing file {df_list["vacancies"]["filename"]} error'
        logger.error(log_message)


p_employers = load_df(CONFIG["DIR_CSV"], df_list["employers"]["filename"])
if p_employers["status"] == 0:
    p_employers["df"].drop_duplicates(inplace=True)
    p_employers["df"].rename(columns={"id": "employer_id"}, inplace=True)
    update_and_insert(
        CONFIG["DB_SCHEMA"],
        df_list["employers"]["db_table"],
        p_employers["df"],
        df_list["employers"]["keys"],
    )
    move_files_processed(
        CONFIG["DIR_CSV"], CONFIG["DIR_CSV_PROCESSED"], df_list["employers"]["filename"]
    )
else:
    log_message = 'ERROR | processing file {df_list["key_skills"]["filename"]} error'
    logger.error(log_message)


p_professional_roles = load_df(
    CONFIG["DIR_CSV"], df_list["professional_roles"]["filename"]
)
if p_professional_roles["status"] == 0:
    p_professional_roles["df"].drop_duplicates(inplace=True)
    update_and_insert(
        CONFIG["DB_SCHEMA"],
        df_list["professional_roles"]["db_table"],
        p_professional_roles["df"],
        df_list["professional_roles"]["keys"],
    )
    move_files_processed(
        CONFIG["DIR_CSV"],
        CONFIG["DIR_CSV_PROCESSED"],
        df_list["professional_roles"]["filename"],
    )
else:
    log_message = 'ERROR | processing file {df_list["key_skills"]["filename"]} error'
    logger.error(log_message)


p_key_skills = load_df(CONFIG["DIR_CSV"], df_list["key_skills"]["filename"])
if p_key_skills["status"] == 0:
    p_key_skills["df"].drop_duplicates(inplace=True)
    update_and_insert(
        CONFIG["DB_SCHEMA"],
        df_list["key_skills"]["db_table"],
        p_key_skills["df"],
        df_list["key_skills"]["keys"],
    )
    move_files_processed(
        CONFIG["DIR_CSV"],
        CONFIG["DIR_CSV_PROCESSED"],
        df_list["key_skills"]["filename"],
    )
else:
    log_message = 'ERROR | processing file {df_list["key_skills"]["filename"]} error'
    logger.error(log_message)


p_industries = load_df(CONFIG["DIR_CSV"], df_list["industries"]["filename"])
if p_industries["status"] == 0:
    p_industries["df"].drop_duplicates(inplace=True)
    update_and_insert(
        CONFIG["DB_SCHEMA"],
        df_list["industries"]["db_table"],
        p_industries["df"],
        df_list["industries"]["keys"],
    )
    move_files_processed(
        CONFIG["DIR_CSV"],
        CONFIG["DIR_CSV_PROCESSED"],
        df_list["industries"]["filename"],
    )
else:
    log_message = 'ERROR | processing file {df_list["industries"]["filename"]} error'
    logger.error(log_message)


p_languages = load_df(CONFIG["DIR_CSV"], df_list["languages"]["filename"])
if p_languages["status"] == 0:
    p_languages["df"].drop_duplicates(inplace=True)
    update_and_insert(
        CONFIG["DB_SCHEMA"],
        df_list["languages"]["db_table"],
        p_languages["df"],
        df_list["languages"]["keys"],
    )
    move_files_processed(
        CONFIG["DIR_CSV"], CONFIG["DIR_CSV_PROCESSED"], df_list["languages"]["filename"]
    )
else:
    log_message = 'ERROR | processing file {df_list["languages"]["filename"]} error'
    logger.error(log_message)
