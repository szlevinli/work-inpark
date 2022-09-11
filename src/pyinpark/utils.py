from enum import IntEnum
from operator import attrgetter, methodcaller
from pathlib import Path
from typing import Callable, Tuple

import arrow
import toolz.curried as tz


class Weekday(IntEnum):
    MON = 0
    TUE = 1
    WED = 2
    THU = 3
    FRI = 4
    SAT = 5
    SUN = 6


@tz.curry
def getLastDateByWeekday(weekday: Weekday, currentDate: arrow.Arrow) -> arrow.Arrow:
    """获取最接近当前的星期几的日期
    
    比如: 今天是2022年9月4日, 获取最近的过去的星期四的日期, 返回2022年9月1日
    """
    return (
        currentDate.shift(days=-1)
        if currentDate.shift(days=-1).weekday() % 7 == weekday.value
        else getLastDateByWeekday(weekday, currentDate.shift(days=-1))
    )


assert (
    getLastDateByWeekday(Weekday.THU, arrow.Arrow(2022, 9, 4)).date()
    == arrow.Arrow(2022, 9, 1).date()
)


assert (
    getLastDateByWeekday(Weekday.THU, arrow.Arrow(2022, 9, 4)).date()
    == arrow.Arrow(2022, 9, 1).date()
)


@tz.curry
def getFileLastName(delimiter: str = ".") -> Callable[[Path], str]:
    """根据分隔符从给定的文件中提取文件名的最后一部分
    
    'a.b.c.sql' 返回 'c'
    """
    return tz.compose(tz.last, methodcaller("split", delimiter), attrgetter("stem"))


assert getFileLastName()(Path("a.b.c.sql")) == "c"
assert getFileLastName()(Path("abc")) == "abc"


@tz.curry
def mapFileToTuple(fn: Callable[[Path], str], file: Path) -> Tuple[str, Path]:
    """映射文件为元组 (str, file)"""
    return (fn(file), file)


assert mapFileToTuple(getFileLastName(), Path("a.b.c.sql"))[0] == "c"

