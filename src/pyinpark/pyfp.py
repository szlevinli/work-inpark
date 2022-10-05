import json
from functools import partial, reduce
from itertools import starmap
from operator import attrgetter, contains, eq, methodcaller
from pathlib import Path
from typing import Callable, Iterable, List, Sequence, Tuple, TypeVar

import toolz.curried as tz

T = TypeVar("T")


map_ = tz.curry(map)

starmap_ = tz.curry(starmap)

filter_ = tz.curry(filter)

eq_ = tz.curry(eq)

contains_ = tz.curry(contains)

reduce_ = tz.curry(reduce)

round_ = tz.flip(round)

min_ = tz.flip(min)

max_ = tz.flip(max)


@tz.curry
def zip_(itr: List[List[T]], last_itr: List[T]):
    """封装python内建函数zip, 实现curry化
    
    >>> zip_([[1,2,3],[4,5,6]])([7,8,9])
    >>> <zip at 0x7fa6a8b004c0>

    >>> list(zip_([[1,2,3],[4,5,6]])([7,8,9])
    >>> [(1, 4, 7), (2, 5, 8), (3, 6, 9)]
    """
    return zip(*itr, last_itr)


@tz.curry
def replace_str(old: str, new: str) -> Callable[[str], str]:
    """curry化str.replace()方法"""
    return methodcaller("replace", old, new)


def split_str(separator: str) -> Callable[[str], List[str]]:
    """curry化str.split()方法"""
    return methodcaller("split", separator)


@tz.curry
def unpack_kwargs(fn, kwargs):
    return fn(**kwargs)


@tz.curry
def read_lines(file_path: Path) -> List[str]:
    lines = []
    with open(file_path) as f:
        lines = [line for line in f]
    return lines


@tz.curry
def dump(file_path: Path, data: dict):
    """将dict对象以JSON的方式写入文件"""
    with open(file_path, "w", encoding="utf8") as f:
        json.dump(data, f, ensure_ascii=False)


# load_jsonc :: Path -> dict
load_jsonc = tz.compose(
    json.loads,
    "".join,
    partial(filter, lambda s: not s.strip().startswith("//")),
    read_lines,
)
# load_jsonc2 = tz.compose(
#     json.loads,
#     "".join,
#     partial(
#         filter,
#         tz.compose(
#             tz.complement(tz.identity),
#             methodcaller("startswith", "//"),
#             methodcaller("strip"),
#         ),
#     ),
#     read_lines,
# )


def get_key_from_filename(separator: str) -> Callable[[Path], str]:
    """从 Path 中提取 key 值

    从文件名中(不含扩展名)按给定的分隔符(`separator`)拆分成字符数组,
    取最后一个元素作为 key 值返回.

    文件名必须有分隔符, 比如: /path1/path2/a.b.c.ext -> c
    """
    return tz.compose(tz.last, methodcaller("split", separator), attrgetter("stem"))


def split_filenames(
    separator: str,
) -> Callable[[Iterable[Path]], Tuple[List[str], List[Path]]]:
    """拆分 Path 对象为 (key, Path) 对

    这部分代码的主要目的是, 将给定的 Path 对象进行拆分便于后续代码使用.

    比如: path1 目录下有 a.b.c1.ext, a.b.c2.ext, a.b.c3.ext 三个文件, 对 path1 进行
    拆分后, 将返回 
    (
        ['c1', 'c2', 'c3'], 
        [
            Path('path1/a.b.c1.ext'), 
            Path(path1/a.b.c2.ext), 
            Path(path1/a.b.c3.ext)
        ]
    )
    """
    return tz.compose(
        tuple,
        map_(list),
        tz.juxt(tz.pluck(0), tz.pluck(1)),
        list,
        # 上面调用 list 的目的是解决 map 返回的 iterator 只能消耗一次的问题
        # 以及以下的代码都是解决同类问题的
        map_(tz.juxt(get_key_from_filename(separator), tz.identity,)),
    )


def eq_suffix(ext: str) -> Callable[[Path], bool]:
    """判断给定的文件扩展名是否与指定的扩展名相同
    
    如果给定的文件是目录, 则其 `suffix` 为 `''`, 所以大部分情况下将返回 `False`
    """
    return tz.compose(eq_(ext), attrgetter("suffix"))


def contains_suffix(exts: List[str]) -> Callable[[Path], bool]:
    """判断给定的文件扩展名是否包含指定的扩展名
    
    如果给定的文件是目录, 则其 `suffix` 为 `''`, 所以大部分情况下将返回 `False`
    """
    return tz.compose(contains_(exts), attrgetter("suffix"))


def extract_key_from_filename(fn_ext: Callable[[str], str]) -> Callable[[Path], str]:
    """从给定的文件对象中提取key

    其中`fn_ext`是提取key的函数, 它接收str作为输入, 返回一个str作为key

    >>> split_and_get_last = tz.compose(tz.last, methodcaller("split", "."))
    >>> extract_key_from_filename(split_and_get_last)("a.b.c.sql")
    >>> 'c'
    """
    return tz.compose(fn_ext, attrgetter("stem"))


def over_all(lst: Sequence):
    """over all sequence
    
    input ['a', 'b', 'c'], return [['a'], ['a', 'b'], ['a', 'b', 'c']]
    """
    return tz.pipe(lst, len, range, tz.map(lambda i: lst[: i + 1]),)

