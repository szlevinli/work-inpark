import requests
import pandas as pd
from pathlib import Path
from typing import Dict, List, NamedTuple, Optional, TypedDict, Any, Union


class RemoteDataMustContainFields(TypedDict):
    rows: List[List[str]]
    column_list: List[str]


RemoteData = Union[RemoteDataMustContainFields, Dict[str, Any]]


class DBArgs(NamedTuple):
    DOMAIN: str
    LOGIN_URL: str
    AUTH_URL: str
    QUERY_URL: str
    DESC_URL: str
    DICT_URL: str
    USR: str
    PWD2: str
    DB_NAME: str
    INSTANCE_NAME: str


class DBClient:
    def __init__(self, db_args: DBArgs) -> None:
        self.args = db_args
        self.session = requests.Session()
        # login
        self.session.get(self.args.LOGIN_URL)
        # auth
        self.session.post(
            self.args.AUTH_URL,
            headers={"X-CSRFToken": self.session.cookies["csrftoken"]},
            data={"username": self.args.USR, "password": self.args.PWD2},
        )

    def query_raw(self, sql: str) -> requests.Response:
        return self.session.post(
            self.args.QUERY_URL,
            headers={"X-CSRFToken": self.session.cookies["csrftoken"]},
            data={
                "db_name": self.args.DB_NAME,
                "instance_name": self.args.INSTANCE_NAME,
                "limit_num": 0,
                "schema_name": "",
                "sql_content": sql,
                "tb_name": "",
            },
        )

    def query(self, sql: str) -> RemoteData:
        return self.session.post(
            self.args.QUERY_URL,
            headers={"X-CSRFToken": self.session.cookies["csrftoken"]},
            data={
                "db_name": self.args.DB_NAME,
                "instance_name": self.args.INSTANCE_NAME,
                "limit_num": 0,
                "schema_name": "",
                "sql_content": sql,
                "tb_name": "",
            },
        ).json()["data"]

    def describe_table(self, db_name: str, tb_name: str) -> requests.Response:
        return self.session.post(
            self.args.DESC_URL,
            headers={"X-CSRFToken": self.session.cookies["csrftoken"]},
            data={
                "db_name": db_name,
                "instance_name": self.args.INSTANCE_NAME,
                "schema_name": "",
                "tb_name": tb_name,
            },
        )

    def data_dictionary(self, db_name: str, tb_name: str) -> RemoteData:
        return self.session.get(
            self.args.DICT_URL,
            headers={"X-CSRFToken": self.session.cookies["csrftoken"]},
            params={
                "db_name": db_name,
                "instance_name": self.args.INSTANCE_NAME,
                "tb_name": tb_name,
            },
        ).json()["data"]["desc"]

    def get_table_structs(self, db_name: str, tb_name: str) -> pd.DataFrame:
        data = self.data_dictionary(db_name=db_name, tb_name=tb_name)
        return pd.DataFrame(data["rows"], columns=data["column_list"])

    def get_select_result(
        self,
        cache_file: Optional[str | Path] = None,
        sql_file: Optional[str | Path] = None,
        sql: Optional[str] = None,
    ) -> pd.DataFrame:
        cache_file_path = Path(cache_file) if cache_file is not None else None

        if cache_file_path is not None and cache_file_path.exists():
            return pd.read_csv(
                cache_file_path, index_col=0, low_memory=False, escapechar="\\"
            )

        sql_ = "select 1"

        if sql is not None and sql.strip() != "":
            sql_ = sql
        elif sql_file is not None and Path(sql_file).exists():
            with open(sql_file, "r") as f:
                sql_ = "".join([line for line in f])

        data = self.query(sql_)

        df = pd.DataFrame(data["rows"], columns=data["column_list"])

        if cache_file_path is not None:
            df.to_csv(cache_file_path, escapechar="\\")

        return df
