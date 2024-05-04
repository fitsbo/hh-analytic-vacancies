# Скрипт для апдейта или создания таблиц со словарями

from helpers.db_helper import update_and_insert
from helpers.df_files import load_df
from helpers.logger import MyLogger

CONFIG = {
    "FILENAME_LOG": "insert_dicts",
    "CONSOLE_LOG": True,
    "CONSOLE_LEVEL_LOG": "info",
    "DIR_DICT": "./dict",
    "DB_SCHEMA": "hh_project",
}

logger = MyLogger(
    name=CONFIG["FILENAME_LOG"],
    stream=CONFIG["CONSOLE_LOG"],
    stream_level=CONFIG["CONSOLE_LEVEL_LOG"],
)


df_list = {
    "dic_region": {
        "db_table": "dic_region",
        "filename": "region.csv",
        "keys": ["name", "type", "okrug"],
        "kwargs": {"delimiter": ";"},
    },
    "dic_cities": {
        "db_table": "dic_cities",
        "filename": "cities.csv",
        "keys": ["city", "type"],
        "kwargs": {"delimiter": ";"},
    },
    "dic_region_salaries": {
        "db_table": "dic_region_salaries",
        "filename": "region_avg_salary_renamed_predicted.csv",
        "keys": ["month", "region_name"],
        "kwargs": {},
    },
}


for d in df_list:
    df = load_df(
        src_directory=CONFIG["DIR_DICT"],
        file_name=df_list[d]["filename"],
        **df_list[d]["kwargs"]
    )
    if df["status"] == 0:
        update_and_insert(
            schema=CONFIG["DB_SCHEMA"],
            table=df_list[d]["db_table"],
            data=df["df"],
            keys=df_list[d]["keys"],
        )
