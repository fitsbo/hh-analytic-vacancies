import json

import requests
from requests.adapters import HTTPAdapter
from urllib3 import Retry

from helpers.logger import MyLogger

CONFIG = {
    "BASE_URL": "https://api.hh.ru",
    "REQUESTS_RETRIES": 8,
    "FILENAME_LOG": "hhapirequest",
    "CONSOLE_LOG": False,
    "CONSOLE_LEVEL_LOG": "info",
}

logger = MyLogger(
    name=CONFIG["FILENAME_LOG"],
    stream=CONFIG["CONSOLE_LOG"],
    stream_level=CONFIG["CONSOLE_LEVEL_LOG"],
)


def get_url(url: str, params=None) -> dict:
    result = {"status": "", "data": None}
    session = requests.Session()
    adapter = HTTPAdapter(
        max_retries=Retry(
            total=CONFIG["REQUESTS_RETRIES"],
            backoff_factor=1,
            # allowed_methods=None,
            status_forcelist=[403, 429, 500, 502, 503, 504],
        )
    )
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    try:
        r = session.get(url=url, params=params)
        if r.status_code == 200:
            result["status"] = "OK"
            result["data"] = r.content
        else:
            result["status"] = "FAIL"
            result["data"] = r.status_code
    except requests.exceptions.RetryError as e:
        result["status"] = "FAIL"
        result["data"] = e
    log_message = f'GET {url} status {result["status"]}'
    logger.info(log_message)
    return result


def get_areas_by_country(country_id):
    result = {}
    url = "{}/areas/{}".format(CONFIG["BASE_URL"], country_id)
    response = get_url(url)
    if response["status"] == "OK":
        data = json.loads(response["data"].decode())
        result = {area["id"]: area["name"] for area in data["areas"]}
        return result
    else:
        log_message = f"Error get areas list: {response['data']}"
        logger.error(log_message)
        raise RuntimeError(log_message)


def get_professional_roles(title_list: list):
    result = {}
    url = "{}/professional_roles".format(CONFIG["BASE_URL"])
    response = get_url(url)
    if response["status"] == "OK":
        data = json.loads(response["data"].decode())
        for category in data["categories"]:
            for title in title_list:
                for role in category["roles"]:
                    if title.lower() in role["name"].lower():
                        if role["id"] not in result.keys():
                            result[role["id"]] = role["name"]
        return result
    else:
        log_message = f"Error get roles list: {response['data']}"
        logger.error(log_message)
        raise RuntimeError(log_message)


def get_vacancies_page(params):
    # result = {}
    url = "{}/vacancies".format(CONFIG["BASE_URL"])
    response = get_url(url, params)
    return response


if __name__ == "__main__":
    # print(get_areas_by_country("113"))
    print(
        get_professional_roles(
            title_list=[
                "analyst",
                "аналитик",
                "data",
                "дата",
                "dwh",
                "etl",
                "bi",
            ]
        )
    )
