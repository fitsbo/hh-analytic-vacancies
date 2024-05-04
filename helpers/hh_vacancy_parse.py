import json

import helpers.hh_api_requests as hh_api_requests
from helpers.logger import MyLogger

CONFIG = {
    "FILENAME_LOG": "hhvacancyparse",
    "CONSOLE_LOG": True,
    "CONSOLE_LEVEL_LOG": "info",
}

logger = MyLogger(
    name=CONFIG["FILENAME_LOG"],
    stream=CONFIG["CONSOLE_LOG"],
    stream_level=CONFIG["CONSOLE_LEVEL_LOG"],
)


def get_pages(params: dict) -> dict:
    result = {"pages": -1, "total": -1, "error": None}
    response = hh_api_requests.get_vacancies_page(params)
    if response["status"] == "OK":
        data = json.loads(response["data"].decode())
        result["pages"] = data["pages"] if data["pages"] <= 100 else 100
        result["total"] = data["found"]
    else:
        result["error"] = response["data"]
    return result


def parse_details(url: str) -> dict:
    result = {
        "error": None,
        "details": {},
        "languages": [],
        "skills": [],
        "role": [],
    }

    response = hh_api_requests.get_url(url)

    if response["status"] == "OK":
        vacancy_json = json.loads(response["data"].decode())

        result["details"]["id"] = vacancy_json["id"]
        result["details"]["name"] = vacancy_json["name"]
        result["details"]["alternate_url"] = vacancy_json["alternate_url"]
        result["details"]["area_id"] = vacancy_json["area"]["id"]
        result["details"]["area_name"] = vacancy_json["area"]["name"]
        result["details"]["type_name"] = vacancy_json["type"]["name"]
        result["details"]["experience"] = vacancy_json["experience"]["name"]
        result["details"]["schedule"] = vacancy_json["schedule"]["name"]
        result["details"]["employment"] = vacancy_json["employment"]["name"]
        result["details"]["accept_handicapped"] = vacancy_json["accept_handicapped"]
        result["details"]["accept_temporary"] = vacancy_json["accept_temporary"]
        result["details"]["created_at"] = vacancy_json["created_at"]
        result["details"]["archived"] = vacancy_json["archived"]
        result["details"]["salary"] = vacancy_json["salary"]
        if vacancy_json["type"]["id"] != "anonymous":
            result["details"]["employer_id"] = vacancy_json["employer"]["id"]
            result["details"]["employer_name"] = vacancy_json["employer"]["name"]
        else:
            result["details"]["employer_id"] = None
            result["details"]["employer_name"] = vacancy_json["employer"]["name"]
        if vacancy_json["salary"] is not None:
            result["details"]["salary_from"] = vacancy_json["salary"]["from"]
            result["details"]["salary_to"] = vacancy_json["salary"]["to"]
            result["details"]["salary_currency"] = vacancy_json["salary"]["currency"]
            result["details"]["salary_gross"] = vacancy_json["salary"]["gross"]
        else:
            result["details"]["salary_from"] = None
            result["details"]["salary_to"] = None
            result["details"]["salary_currency"] = None
            result["details"]["salary_gross"] = None

        result["languages"] = [
            {
                "vacancy_id": (
                    vacancy_json["id"] if vacancy_json["id"] is not None else None
                ),
                "language": _["name"] if _["name"] is not None else None,
                "level": (
                    _["level"]["name"] if _["level"]["name"] is not None else None
                ),
            }
            for _ in vacancy_json["languages"]
        ]

        result["skills"] = [
            {
                "vacancy_id": vacancy_json["id"],
                "skill": _["name"],
            }
            for _ in vacancy_json["key_skills"]
        ]

        result["role"] = [
            {
                "vacancy_id": vacancy_json["id"],
                "role_id": _["id"],
                "role_name": _["name"],
            }
            for _ in vacancy_json["professional_roles"]
        ]
    else:
        result["error"] = response["data"]
    return result


def parse_employer(url: str) -> dict:
    result = {
        "error": None,
        "details": {},
        "industries": [],
    }

    response = hh_api_requests.get_url(url)

    if response["status"] == "OK":
        employer_json = json.loads(response["data"].decode())
        result["details"]["id"] = employer_json["id"]
        result["details"]["alternate_url"] = employer_json["alternate_url"]
        result["details"]["name"] = employer_json["name"]
        result["details"]["type"] = employer_json["type"]
        result["details"]["site_url"] = employer_json["site_url"]
        result["details"]["area_id"] = employer_json["area"]["id"]
        result["details"]["area_name"] = employer_json["area"]["name"]
        result["details"]["open_vacancies"] = employer_json["open_vacancies"]
        result["industries"] = [
            {
                "employer_id": employer_json["id"],
                "industry_id": _["id"],
                "industry_name": _["name"],
            }
            for _ in employer_json["industries"]
        ]
    else:
        result["error"] = response["data"]
    return result
