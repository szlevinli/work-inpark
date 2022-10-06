"""command line interface
"""

import argparse
from operator import attrgetter

import arrow
import toolz.curried as tz

from ..pyinpark.utils import getLastDateByWeekday, Weekday

parser = argparse.ArgumentParser()

# set command line arguments
# ==================

# statistics day
arg_stat_name = "statistics_day"
parser.add_argument(
    "-s",
    f"--{arg_stat_name}",
    default=Weekday.THU.name,
    choices=[name for name in Weekday.__members__],
    help="统计日. 比如: 选择周四作为统计日",
)

# data directory
arg_data_name = "data_dir"
parser.add_argument(
    "-d", f"--{arg_data_name}", default="data", help="数据存放路径. 基于当前执行路径."
)

# out directory
arg_out_name = "out_dir"
parser.add_argument(
    "-o", f"--{arg_out_name}", default="out", help="输出文件存放路径. 基于当前执行路径."
)


# get command line arguments
# ==================

args = parser.parse_args()

statistics_day = tz.pipe(args, attrgetter(arg_stat_name))
data_dir = tz.pipe(args, attrgetter(arg_data_name))
out_dir = tz.pipe(args, attrgetter(arg_out_name))


if __name__ == "__main__":
    print(statistics_day, data_dir, out_dir)
