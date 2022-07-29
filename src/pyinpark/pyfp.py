import toolz.curried as tz
from typing import List, Iterable, Any
from pathlib import Path
import json


@tz.curry
def zip_(itr, last_itr):
    return zip(*itr, last_itr)


@tz.curry
def unpack_kwargs(fn, kwargs):
    return fn(**kwargs)


@tz.curry
def join(s: str, itr: Iterable[Any]) -> str:
    return s.join(itr)


@tz.curry
def read_lines(file_path: Path) -> List[str]:
    lines = []
    with open(file_path) as f:
        lines = [line for line in f]
    return lines


@tz.curry
def dump(file_path: Path, data: dict):
    with open(file_path, "w", encoding="utf8") as f:
        json.dump(data, f, ensure_ascii=False)
