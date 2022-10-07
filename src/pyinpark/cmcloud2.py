import requests
import os
from dotenv import load_dotenv

load_dotenv()


def login():
    return requests.get(os.getenv("LOGIN_URL"))


def auth(login_res):
    return requests.post(
        os.getenv("AUTH_URL"),
        headers={"X-CSRFToken": login_res.cookies["csrftoken"],},
        cookies=login_res.cookies,
        # cspell: disable-next-line
        data={"username": os.getenv("USR"), "password": os.getenv("PWD2")},
    )


def query(auth_res, sql):
    return requests.post(
        os.getenv("QUERY_URL"),
        headers={"X-CSRFToken": auth_res.cookies["csrftoken"],},
        cookies=auth_res.cookies,
        data={
            "db_name": os.getenv("DB_NAME"),
            "instance_name": os.getenv("INSTANCE_NAME"),
            "limit_num": 0,
            "schema_name": "",
            # cspell: disable-next-line
            "sql_content": sql,
            "tb_name": "",
        },
    )

