import requests
import os
import pandas as pd
from pathlib import Path
from typing import Dict, List, NamedTuple, Optional, TypedDict, Any, Union


class RemoteDataMustContainFields(TypedDict):
    rows: List[List[str]]
    column_list: List[str]


RemoteData = Union[RemoteDataMustContainFields, Dict[str, Any]]


class DbArgs(NamedTuple):
    login_url: str = ""
    auth_url: str = ""
    desc_url: str = ""
    dict_url: str = ""
    usr: str = ""
    pwd: str = ""
    query_url: str = ""
    db_name: str = ""
    instance_name: str = ""


def getEnv(k: str) -> str:
    v = os.getenv(k)
    return v if v is not None else ""


def load_args_from_env() -> DbArgs:
    return DbArgs(
        login_url=getEnv("LOGIN_URL"),
        auth_url=getEnv("AUTH_URL"),
        desc_url=getEnv("DESC_URL"),
        dict_url=getEnv("DICT_URL"),
        usr=getEnv("USR"),
        pwd=getEnv("PWD2"),
        query_url=getEnv("QUERY_URL"),
        db_name=getEnv("DB_NAME"),
        instance_name=getEnv("INSTANCE_NAME"),
    )


class DBClient:
    def __init__(self, db_args: DbArgs) -> None:
        self.args = db_args
        self.session = requests.Session()
        # login
        self.session.get(self.args.login_url)
        # auth
        self.session.post(
            self.args.auth_url,
            headers={"X-CSRFToken": self.session.cookies["csrftoken"]},
            data={"username": self.args.usr, "password": self.args.pwd},
        )

    def query(self, sql: str) -> RemoteData:
        return self.session.post(
            self.args.query_url,
            headers={"X-CSRFToken": self.session.cookies["csrftoken"]},
            data={
                "db_name": self.args.db_name,
                "instance_name": self.args.instance_name,
                "limit_num": 0,
                "schema_name": "",
                "sql_content": sql,
                "tb_name": "",
            },
        ).json()["data"]

    def describe_table(self, db_name: str, tb_name: str) -> requests.Response:
        return self.session.post(
            self.args.desc_url,
            headers={"X-CSRFToken": self.session.cookies["csrftoken"]},
            data={
                "db_name": db_name,
                "instance_name": self.args.instance_name,
                "schema_name": "",
                "tb_name": tb_name,
            },
        )

    def data_dictionary(self, db_name: str, tb_name: str) -> RemoteData:
        return self.session.get(
            self.args.dict_url,
            headers={"X-CSRFToken": self.session.cookies["csrftoken"]},
            params={
                "db_name": db_name,
                "instance_name": self.args.instance_name,
                "tb_name": tb_name,
            },
        ).json()["data"]["desc"]

    def get_table_structs(self, db_name: str, tb_name: str) -> pd.DataFrame:
        data = self.data_dictionary(db_name=db_name, tb_name=tb_name)
        return pd.DataFrame(data["rows"], columns=data["column_list"])

    def get_select_result(
        self,
        cache_file: str,
        sql_file: Optional[str] = None,
        sql: Optional[str] = None,
    ) -> pd.DataFrame:
        cache_file_path = Path(cache_file)

        if cache_file_path.exists():
            return pd.read_csv(cache_file_path, index_col=0, low_memory=False)

        sql_ = "select 1"

        if sql is not None and sql.strip() != "":
            sql_ = sql
        elif sql_file is not None and Path(sql_file).exists():
            with open(sql_file, "r") as f:
                sql_ = "".join([line for line in f])

        data = self.query(sql_)

        df = pd.DataFrame(data["rows"], columns=data["column_list"])

        df.to_csv(cache_file_path)

        return df
