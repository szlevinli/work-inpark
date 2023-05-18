from operator import itemgetter, methodcaller
import requests
import toolz.curried as tz
import os
import pandas as pd
from pathlib import Path


def login(login_url):
    return requests.get(login_url)


@tz.curry
def auth(auth_url, usr, pwd, login_res):
    return requests.post(
        auth_url,
        headers={
            "X-CSRFToken": login_res.cookies["csrftoken"],
        },
        cookies=login_res.cookies,
        # cspell: disable-next-line
        data={"username": usr, "password": pwd},
    )


@tz.curry
def query(query_url, db_name, instance_name, auth_res, sql):
    return requests.post(
        query_url,
        headers={
            "X-CSRFToken": auth_res.cookies["csrftoken"],
        },
        cookies=auth_res.cookies,
        data={
            "db_name": db_name,
            "instance_name": instance_name,
            "limit_num": 0,
            "schema_name": "",
            # cspell: disable-next-line
            "sql_content": sql,
            "tb_name": "",
        },
    )


def create_sql_executor_for_leaseRent(load_env_function):
    load_env_function()

    login_url = os.getenv("LOGIN_URL")
    auth_url = os.getenv("AUTH_URL")
    usr = os.getenv("USR")
    pwd = os.getenv("PWD2")
    query_url = os.getenv("QUERY_URL")
    db_name = os.getenv("DB_NAME")
    instance_name = os.getenv("INSTANCE_NAME")

    return tz.pipe(
        login_url,
        login,
        auth(auth_url, usr, pwd),
        query(query_url, db_name, instance_name),
    )


def get_data_from_remote(executor, sql_file=None, sql="select 1"):
    _sql = sql

    if sql_file is not None:
        with open(sql_file, "r") as f:
            _sql = "".join([line for line in f])

    return tz.pipe(_sql, executor, methodcaller("json"), itemgetter("data"))


def create_df(data_file_path, sql_file, sql_executor, force_update=False):
    file_path = Path(data_file_path)

    if file_path.exists() and not force_update:
        return pd.read_csv(file_path, index_col=0, low_memory=False)

    data = get_data_from_remote(sql_executor, sql_file)

    df = pd.DataFrame(data["rows"], columns=data["column_list"])  # type: ignore

    df.to_csv(file_path)

    return df
