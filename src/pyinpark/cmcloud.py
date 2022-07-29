import urllib3
import toolz.curried as tz
import json
from http import cookies
from operator import methodcaller, attrgetter
from collections import namedtuple
from pyinpark.pyfp import zip_, unpack_kwargs  # cspell: disable-line

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

HOST = "msdms.cmsk1979.com"  # cspell: disable-line
Endpoints = namedtuple("Endpoints", "login authenticate query")
urls = Endpoints(*[f"https://{HOST}/{endpoint}/" for endpoint in Endpoints._fields])

timeout = urllib3.util.Timeout(connect=2.0, read=7.0)

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
    "username": "lihaiqiang",  # cspell: disable-line
    "password": "Cmsk@2022lhq",  # cspell: disable-line
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
    "db_name": "yydbs",  # cspell: disable-line
    "instance_name": "rents100.97.160.18",
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


# query_request :: dict -> str -> JSON
@tz.curry
def query_request(headers):
    return tz.compose(
        json.loads,
        attrgetter("data"),
        unpack_kwargs(http.request),
        tz.assoc_in(headers, ["fields", "sql_content"]),
    )

