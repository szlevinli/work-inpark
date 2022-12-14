import urllib3
import toolz.curried as tz
import json
import os
from dotenv import load_dotenv, find_dotenv
from http import cookies
from operator import methodcaller, attrgetter
from collections import namedtuple
from typing import Any, Callable, cast
from pyinpark.pyfp import zip_, unpack_kwargs  # cspell: disable-line

load_dotenv()
env_path = find_dotenv()

# output_cookies :: SimpleCookie -> str
output_cookies = tz.compose(
    methodcaller("strip"), methodcaller("output", attrs=[], header="", sep=";")
)


# get_cookies_from_headers :: HTTPResponse -> str
get_cookies_from_headers = methodcaller("getheader", "set-cookie")

# simpleCookie :: HTTPResponse -> SimpleCookie
simpleCookie = tz.compose(cookies.SimpleCookie, get_cookies_from_headers)


# get_cookies_output :: HTTPResponse -> str
get_cookies_output = tz.compose(output_cookies, simpleCookie)

# get_csrfToken :: HTTPResponse -> str
get_csrfToken = tz.compose(attrgetter("value"), tz.get("csrftoken"), simpleCookie)

# set_headers :: HTTPResponse -> dict
get_headers = tz.compose(
    dict,
    zip_([["Cookie", "X-CSRFToken"]]),
    tz.juxt([get_cookies_output, get_csrfToken]),
)

HOST = os.getenv("DOMAIN")
Endpoints = namedtuple("Endpoints", "login authenticate query")
urls = Endpoints(*[f"https://{HOST}/{endpoint}/" for endpoint in Endpoints._fields])

timeout = urllib3.util.Timeout(connect=2.0, read=30.0)

http = urllib3.PoolManager(timeout=timeout)

#
# login
#

# login :: _ -> HTTPResponse
login = lambda: http.request("GET", urls.login)

#
# authentication
#


auth_fields = {
    "username": os.getenv("USR"),  # cspell: disable-line
    "password": os.getenv("PWD"),  # cspell: disable-line
}

auth_request_args = {
    "method": "POST",
    "url": urls.authenticate,
    "headers": "",
    "fields": auth_fields,
}

# auth :: HTTPResponse -> HTTPResponse
auth = tz.compose(
    unpack_kwargs(http.request),
    tz.assoc_in(auth_request_args, ["headers"]),
    get_headers,
)

#
# query
#


query_fields = {
    "db_name": os.getenv("DB_NAME"),
    "instance_name": os.getenv("INSTANCE_NAME"),
    "limit_num": 0,
    "schema_name": "",
    "sql_content": "select 1",
    "tb_name": "",
}

query_request_args = {
    "method": "POST",
    "url": urls.query,
    "headers": "",
    "fields": query_fields,
}

# get_query_headers :: HTTPResponse -> dict
get_query_headers = tz.compose(
    tz.assoc_in(query_request_args, ["headers"]), get_headers
)

# get_headers_immediately :: _ -> dict
get_headers_immediately = cast(
    Callable[[], Any], tz.compose(get_query_headers, auth, login)
)


# query_request :: dict -> str -> JSON
@tz.curry
def query_request(headers):
    return tz.compose(
        json.loads,
        attrgetter("data"),
        unpack_kwargs(http.request),
        tz.assoc_in(headers, ["fields", "sql_content"]),
    )

