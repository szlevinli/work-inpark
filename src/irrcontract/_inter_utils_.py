from collections import namedtuple
from pathlib import Path
from typing import List

import arrow
import toolz.curried as tz
from pyinpark.utils import Weekday, getLastDateByWeekday

from constants import DATE_FORMAT


def getDate(d: Weekday):
    """get exec, statistics, last statistics date

    return tuple (exeDate, statDate, lastStatDate)
    """
    return tz.pipe(
        tz.iterate(getLastDateByWeekday(d), arrow.now().floor("day")), tz.take(3),
    )


def getPaths(paths: List[str], statDate: arrow.Arrow):
    root = Path.cwd()
    Paths = namedtuple("Paths", tz.cons("root", paths))
    return Paths._make(
        [
            root
            if name == "root"
            else tz.pipe(
                root / name / statDate.format(DATE_FORMAT),
                tz.do(lambda p: p.mkdir(parents=True, exist_ok=True)),
            )
            for name in Paths._fields
        ]
    )