import numpy as np
import pandas as pd
import psycopg2

from helpers.logger import MyLogger

CONFIG = {
    "CONN_DB": "",
    "CONN_USER": "",
    "CONN_PASSWORD": "",
    "CONN_HOST": "",
    "CONN_PORT": "",
    "FILENAME_LOG": "dbhelper",
    "CONSOLE_LOG": True,
    "CONSOLE_LEVEL_LOG": "info",
}

logger = MyLogger(
    name=CONFIG["FILENAME_LOG"],
    stream=CONFIG["CONSOLE_LOG"],
    stream_level=CONFIG["CONSOLE_LEVEL_LOG"],
)


def get_conn():
    conn = psycopg2.connect(
        database=CONFIG["CONN_DB"],
        user=CONFIG["CONN_USER"],
        password=CONFIG["CONN_PASSWORD"],
        host=CONFIG["CONN_HOST"],
        port=CONFIG["CONN_PORT"],
    )
    log_message = f'OK | DB connection to {CONFIG["CONN_DB"]} created'
    logger.debug(log_message)
    return conn


def update(conn, schema: str, table: str, row: pd.DataFrame, keys: list):
    # update_filters = " AND ".join()
    update_filters = " AND ".join([f"{key} = %s" for key in keys])
    row_values = [row[key] for key in keys]

    update_query = f"""
        UPDATE {schema}.{table}
        SET valid_dttm = CURRENT_TIMESTAMP
        WHERE valid_dttm = '5999-12-31'
        AND {update_filters}
    """.strip()

    cursor = conn.cursor()
    try:
        cursor.execute(update_query, row_values)
        conn.commit()
        log_message = "OK | Update VALID_DTTM into {schema}.{table} query executed"
        logger.debug(log_message)
        return 0
    except (Exception, psycopg2.DatabaseError) as error:
        conn.rollback()
        # cursor.close()
        log_message = (
            f"ERROR | Update VALID_DTTM into {schema}.{table} query error {error}"
        )
        logger.error(log_message)
        return 1


def insert(conn, schema: str, table: str, row: pd.DataFrame):
    table_cols = ", ".join(list(row.index))
    insert_placeholders = ", ".join(["%s"] * len(list(row.index)))
    row_values = [value for value in row]

    insert_query = f"""
        INSERT INTO {schema}.{table} ({table_cols})
        VALUES ({insert_placeholders})
    """.strip()

    cursor = conn.cursor()
    try:
        cursor.execute(insert_query, row_values)
        conn.commit()
        log_message = "OK | Insert into {schema}.{table} query executed"
        logger.debug(log_message)
        return 0
    except (Exception, psycopg2.DatabaseError) as error:
        conn.rollback()
        cursor.close()
        log_message = f"ERROR | Insert into {schema}.{table} query error {error}"
        logger.error(log_message)
        return 1


def update_and_insert(schema: str, table: str, data: pd.DataFrame, keys: list):
    data = data.replace({np.nan: None})
    result = {
        "update_errors": 0,
        "insert_errors": 0,
        "total_rows": len(data),
        "success_rows": 0,
    }
    log_message = f'UPDATE_INSERT | START update and insert into {schema}.{table} for {result["total_rows"]} rows'
    logger.info(log_message)
    with get_conn() as conn:
        for index, row in data.iterrows():
            query_result = update(conn, schema, table, row, keys)
            if query_result == 1:
                result["update_errors"] += 1
            else:
                result["success_rows"] += 1
            if query_result == 0:
                query_result = insert(conn, schema, table, row)
                if query_result == 1:
                    result["update_errors"] += 1
                else:
                    result["success_rows"] += 1
    log_message = f'UPDATE_INSERT | FINISH update into {schema}.{table} errors: {result["update_errors"]}, insert errors {result["insert_errors"]}, success rows {result["success_rows"]}'
    logger.info(log_message)
