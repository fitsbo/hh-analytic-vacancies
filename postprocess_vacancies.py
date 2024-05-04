from pathlib import Path

import pandas as pd

import helpers.df_files as df_files
from helpers.logger import MyLogger

CONFIG = {
    "FILENAME_LOG": "postprocess",
    "CONSOLE_LOG": True,
    "CONSOLE_LEVEL_LOG": "info",
    "DIR_CSV": "./csv",
    "DIR_CSV_PROCESSED": "./csv/processed",
    "DIR_DICT": "./dict",
    "NET_RATE": 0.87,
    "CBR_URL": "http://www.cbr.ru/scripts/XML_daily.asp?date_req={}/{}/{}",
}

TITLE_LIST = (
    "analyst",
    "аналитик",
    "data",
    "дата",
    "dwh",
    "etl",
    "bi",
)

RATES_DICT = {}

logger = MyLogger(
    name=CONFIG["FILENAME_LOG"],
    stream=CONFIG["CONSOLE_LOG"],
    stream_level=CONFIG["CONSOLE_LEVEL_LOG"],
)


def fix_region(region, dict_regions):
    if region in dict_regions.keys():
        return dict_regions[region]["region_dict"]
    else:
        return region


def get_cbr_currency(row, rates):
    date = pd.to_datetime(row["created_at"])
    formatted_date = date.strftime("%d/%m/%Y")
    url = CONFIG["CBR_URL"].format(*formatted_date.split("/"))
    key = ",".join((formatted_date, row["salary_currency"]))
    if key in rates.keys():
        row["salary_from"] = round(row["salary_from"] * rates[key], 2)
        row["salary_to"] = round(row["salary_to"] * rates[key], 2)
        log_message = f'OK | Convert rate for vacancy f{row["vacancy_id"]}, {row["salary_currency"]} success. Rate found in dict.'
        logger.debug(log_message)
    else:
        try:
            currencies = pd.read_xml(url, encoding="cp1251")
            rate = (
                currencies.loc[currencies["CharCode"] == row["salary_currency"]][
                    "Value"
                ]
                .str.replace(",", ".")
                .astype("float")
            ).values[0]
            row["salary_from"] = round(row["salary_from"] * rate, 2)
            row["salary_to"] = round(row["salary_to"] * rate, 2)
            rates[key] = rate
            log_message = f'OK | Convert rate for vacancy f{row["vacancy_id"]}, {row["salary_currency"]} success. Rate parsed from web.'
            logger.debug(log_message)
        except Exception as error:
            row["salary_from"] = pd.NA
            row["salary_to"] = pd.NA
            log_message = (
                f'ERROR | Convert rate for vacancy f{row["vacancy_id"]} error: {error}'
            )
            logger.debug(log_message)
    return row


def get_title_category(name):
    category_dict = {
        "Бизнес": ("бизнес", "business", "системн", "system", "sistem", "процесс"),
        "Данные": (
            "scientist",
            "science",
        ),
        "ML": (
            "machine learning",
            "интеллект",
            "chatgpt",
            "learning",
            "ml",
            "cv",
            "ai",
        ),
        "BI": ("bi", "отчёт", "tableau", "отчет", "dashboard", "superset", "power bi"),
        "DE": ("engineer", "инженер", "etl", "dwh"),
        "Продукт": ("продукт", "product", "рынк", "маркетинг", "реклам", "продаж"),
        "Финанс": (
            "кредит",
            "finance",
            "риск",
            "эконом",
            "финанс",
            "инвестиц",
            "портфель",
            "антифрод",
        ),
        "1C": ("1C"),
    }
    for category in category_dict:
        for title in category_dict[category]:
            if title in name.lower():
                return category
    else:
        return "Другое"


def process_vacancies(src_directory: str, file_name: str):
    read_result = {"vacancy_read": 0, "region_dict_read": 0}
    read_file = df_files.load_df(
        src_directory=src_directory,
        file_name=file_name,
        parse_dates=["created_at"],
    )
    if read_file["status"] == 0:
        v_df = read_file["df"]
        read_result["vacancy_read"] = 1
    else:
        log_message = "ERROR | processing file {file_name} error"
        logger.error(log_message)

    # Список регионов из ХХ для матча с геокодами\зарплатами
    read_file = df_files.load_df(
        src_directory=CONFIG["DIR_DICT"],
        file_name="fix_regions.csv",
        index_col=["region_hh"],
        encoding="cp1251",
        delimiter=";",
    )

    if read_file["status"] == 0:
        dict_regions = read_file["df"].to_dict("index")
        read_result["region_dict_read"] = 1
    else:
        log_message = "ERROR | processing file {file_name} error"
        logger.error(log_message)

    if read_result["vacancy_read"] == 1 and read_result["region_dict_read"] == 1:
        # удаляем дубли вакансий
        v_df = v_df.drop_duplicates(subset=["id"])

        # переименование колонки id чтобы избежать конфлитка с индексом в БД
        v_df.rename(columns={"id": "vacancy_id"}, inplace=True)

        # фильтруем только те, что содержат в своём названии ключевики из TITLE_LIST
        v_df = v_df[
            v_df["name"].str.contains("|".join(TITLE_LIST), regex=True, case=False)
        ]

        # Фикс разницы названий региона
        v_df["region_name"] = v_df["region_name"].apply(
            fix_region, args=(dict_regions,)
        )

        # категоризуем вакансии на дата, бизнес, продуктовый и прочих аналитиков
        v_df["title_category"] = v_df["name"].apply(get_title_category)

        v_df.loc[
            (v_df["salary_from"].notnull()) & (v_df["salary_to"].isnull()), "salary_to"
        ] = v_df["salary_from"]
        v_df.loc[
            (v_df["salary_to"].notnull()) & (v_df["salary_from"].isnull()),
            "salary_from",
        ] = v_df["salary_to"]

        # пересчитаем зарплаты указанные до вычета, чтобы уравнять с теми где указано после вычета
        v_df.loc[v_df["salary_gross"] == True, "salary_from"] = round(
            (v_df["salary_from"] * CONFIG["NET_RATE"]), 2
        )
        v_df.loc[v_df["salary_gross"] == True, "salary_to"] = round(
            (v_df["salary_to"] * CONFIG["NET_RATE"]), 2
        )

        # Для зарплат в валюте сохраняем значение именно в валюте
        filter = (v_df["salary_currency"].notnull()) & (
            v_df["salary_currency"] != "RUR"
        )
        v_df["salary_from_currency"] = pd.NA
        v_df["salary_to_currency"] = pd.NA
        v_df["salary_avg_currency"] = pd.NA

        v_df.loc[filter, "salary_from_currency"] = v_df["salary_from"]
        v_df.loc[filter, "salary_to_currency"] = v_df["salary_to"]
        v_df.loc[
            (v_df["salary_to_currency"].notnull())
            & (v_df["salary_from_currency"].notnull()),
            "salary_avg_currency",
        ] = (v_df["salary_to_currency"] + v_df["salary_from_currency"]) / 2

        # Сконвертируем валютные зарплаты в рубли
        v_df[filter] = v_df[filter].apply(
            lambda row: get_cbr_currency(row=row, rates=RATES_DICT), axis=1
        )

        # посчитаем среднюю зарплату для каждой вакансии
        v_df["salary_avg"] = pd.NA
        v_df.loc[
            (v_df["salary_to"].notnull()) & (v_df["salary_from"].notnull()),
            "salary_avg",
        ] = (v_df["salary_to"] + v_df["salary_from"]) / 2

        filename = "".join((Path(file_name).stem, "_parsed", ".csv"))
        df_files.write_df(datasource=v_df, dst_directory=src_directory, name=filename)
        return filename
    else:
        log_message = "ERROR | Read and process file {src_directory}\\{file_name}"
        logger.error(log_message)
        return None
