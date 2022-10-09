# %%
import argparse
from collections import namedtuple
from functools import partial
from operator import attrgetter, itemgetter, methodcaller, truth, not_, contains, eq
from pathlib import Path
import json
from typing import List, Optional, TypedDict, Dict, Callable, Iterator, Tuple

import arrow
import toolz.curried as tz
import pandas as pd
import numpy as np
from dotenv import load_dotenv, find_dotenv
from pyinpark.cmcloud2 import auth, login, query
from pyinpark.utils import Weekday, getLastDateByWeekday
from pyinpark.pdfp import (
    create_df_from_json,
    get_value_by_booleans,
    eq_series,
)
from pyinpark.pyfp import over_all

# %%
# load environment variables

load_dotenv()
find_dotenv()

# %%
# 常量
# ====

DATE_FORMAT = "YYYYMMDD"

ORGS = ["division", "branch", "dept", "project"]
DIVISION = ORGS[:1]
BRANCH = ORGS[:2]
DEPT = ORGS[:3]
PROJECT = ORGS[:4]

CATEGORY = "category"
IRR_CATEGORY = "irr_category"
CATEGORIES = [CATEGORY, IRR_CATEGORY]


# %%
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


# %%
# get command line arguments
# ==================

args = parser.parse_args([])

statistics_day = tz.pipe(args, attrgetter(arg_stat_name))
data_dir = tz.pipe(args, attrgetter(arg_data_name))
out_dir = tz.pipe(args, attrgetter(arg_out_name))


# %%
# set statistics date
# ===================

statDate, lastStatDate = tz.pipe(
    tz.iterate(getLastDateByWeekday(Weekday[statistics_day]), arrow.now().floor("day")),
    tz.take(3),
    list,
    tz.get([1, 2]),
)

# %%
# set directories
# ===================

root = Path(__file__).parent
Paths = namedtuple("Paths", [data_dir, out_dir])
paths = Paths._make(
    [
        tz.pipe(
            root / name / statDate.format(DATE_FORMAT),
            tz.do(lambda p: p.mkdir(parents=True, exist_ok=True)),
        )
        for name in Paths._fields
    ]
)

# %%
# query data from remote
# ======================

sql_files = tz.pipe(root / "sql", methodcaller("glob", "*.sql"), list)
sql_keys = tz.pipe(
    sql_files,
    tz.map(tz.compose(tz.last, methodcaller("split", "."), attrgetter("stem"))),
    list,
)
sql_executes = tz.pipe(
    sql_files,
    tz.map(
        tz.compose(
            methodcaller("replace", "__END_DATE__", statDate.format(DATE_FORMAT)),
            methodcaller("read_text"),
        )
    ),
    list,
)


# %%
auth_res = tz.pipe(login(), auth)
exec_query = partial(query, auth_res)

# %%
Dfs = namedtuple("Dfs", tz.pipe(sql_keys, sorted))
dfs = (
    Dfs._make(
        [
            tz.pipe(
                sql,
                exec_query,
                methodcaller("json"),
                # 服务器响应的数据落盘便于问题排查
                tz.do(
                    partial(
                        json.dump,
                        fp=(paths.data / f"{key}.response.json").open(mode="w"),
                        ensure_ascii=False,
                    )
                ),
                itemgetter("data"),
                create_df_from_json("rows", "column_list"),
                # # 转存`df`到磁盘
                tz.do(methodcaller("to_csv", paths.data / f"{key}.csv", index=False)),
            )
            for key, sql in tz.pipe(
                zip(sql_keys, sql_executes), partial(sorted, key=tz.first)
            )
        ]
    )
    if tz.pipe(paths.data.glob("*.csv"), list, truth, not_)
    else Dfs._make([pd.read_csv(csv) for csv in sorted(paths.data.glob("*.csv"))])
)

# %%
# 读取配置文件, 白名单和历史所有不规范合同清单
# =========================================

config = json.load((root / "config/config.fp3.json").open())
whiteList = pd.read_excel(root / "config/whitelist.xlsx")
allContractsPath = root / "config/allContracts.xlsx"
allContracts = pd.read_excel(allContractsPath)


# %%
# 处理当期数据
# ===========

# 项目部: 无锡太湖新城
DPT_ID = 1437241
# 公寓项目: 武汉东湖公寓(壹间.东湖网谷), 重庆九龙公寓
PRJ_IDS = [1437202, 1436221]

dfTp = (
    # auto convert data type
    dfs.contract.convert_dtypes()
    # convert to date type
    .assign(
        **{
            k: pd.to_datetime(dfs.contract[k], errors="ignore")
            for k in dfs.contract.columns
            if ("_date" in k) | ("_time" in k)
        }
    )
    # 无锡太湖新城
    .pipe(lambda df: df[df["dept_id"] != DPT_ID])
    # 合同倒签
    .assign(
        # 武汉东湖公寓(壹间.东湖网谷) 重庆九龙公寓 +5 days
        # 工位, 场地类型的合同 + 5 days
        compute_date=lambda df: np.where(
            (df["category"] == "RD")
            & (
                (df["project_id"].isin(PRJ_IDS))
                | (df["contract_id"].isin(dfs.resPurpose["contract_id"]))
            ),
            df["condition_date"] + pd.DateOffset(5),
            df["compute_date"],
        ),
        irr_category=lambda df: np.where(
            (df["category"] == "RD") & (df["apply_approve_date"] > df["compute_date"]),
            "RN",
            df[IRR_CATEGORY],
        ),
    )
    # 应结未结
    .assign(
        # 武汉东湖公寓(壹间.东湖网谷) 重庆九龙公寓 +5 days
        compute_date=lambda df: np.where(
            (df[CATEGORY] == "TD") & (df["project_id"].isin(PRJ_IDS)),
            df["condition_date"] + pd.DateOffset(5),
            df["compute_date"],
        ),
        irr_category=lambda df: np.where(
            (df[CATEGORY] == "TD") & (df["apply_approve_date"] > df["compute_date"]),
            "TN",
            df[IRR_CATEGORY],
        ),
    )
    # 应算未算
    .assign(
        irr_category=lambda df: np.where(
            (df[CATEGORY] == "SD")
            & (df["apply_approve_date"] > df["compute_date"])
            & (df["contract_id"].isin(dfs.unsettlement["obj_id"])),
            "SN",
            df[IRR_CATEGORY],
        ),
    )
    # 应用白名单
    .assign(
        irr_category=lambda df: np.where(
            df.set_index(["contract_no", IRR_CATEGORY]).index.isin(
                whiteList.set_index(["contract_no", IRR_CATEGORY]).index
            ),
            None,
            df[IRR_CATEGORY],
        )
    )
)

# %%
# 当期数据落盘
# ===========

pd.concat(
    [
        allContracts[allContracts["statistic_date"].dt.date != statDate.date()],
        dfTp.assign(statistic_date=pd.to_datetime(statDate.date())),
    ]
).to_excel(allContractsPath, index=False)

# %%
# 全量不合规范合同
# ===============

dfIrr = dfTp[dfTp[IRR_CATEGORY].notna()].assign(
    statistic_date=pd.to_datetime(statDate.date()),
    reason=lambda df: np.where(
        df[IRR_CATEGORY] == "SN",
        df["contract_no"].map(
            tz.compose(
                lambda v: f"应收: {v / 100:,.2f}" if v > 0 else f"应退: {-v / 100:,.2f}",
                get_value_by_booleans(0, dfs.unsettlement["owe_fee"]),
                eq_series(dfs.unsettlement["contract_no"]),
            )
        ),
        df[IRR_CATEGORY].map(lambda v: "合同开始日期: " if v == "RN" else "合同终止日期: ")
        + df["condition_date"].dt.strftime("%Y-%m-%d")
        + df[IRR_CATEGORY].map(lambda _: "\n流程审定日期: ")
        + df["apply_approve_date"].dt.strftime("%Y-%m-%d"),
    ),
)


# %%
# 本期新增不规范合同
# =================

dfIncrease = dfIrr[
    ~(
        dfIrr.set_index(["contract_no", IRR_CATEGORY]).index.isin(
            allContracts[allContracts["statistic_date"].dt.date == lastStatDate.date()]
            .set_index((["contract_no", IRR_CATEGORY]))
            .index
        )
    )
]


# %%
# 组织机构与不合规范合同类型全连接
# ==============================

dfIrrTypeWithPrj = pd.merge(
    dfs.organization, dfIrr.drop_duplicates(subset=CATEGORIES)[CATEGORIES], how="cross",
)


# %%
# 按组织机构统计合同数据
# ====================


@tz.curry
def countContractByOrg(
    orgPath: List[str], cate: List[str], fieldName: str, dfIn: pd.DataFrame
) -> pd.DataFrame:
    return (
        dfIn.groupby(by=(orgPath + cate), dropna=False)[[fieldName]]
        .count()
        .rename(columns={fieldName: "irr"})
        .assign(
            total=lambda df: df.groupby(level=df.index.names[:-1])["irr"].transform(
                "sum"
            ),
            rate=lambda df: round(df["irr"] / df["total"], 4),
        )
    )


Counts = namedtuple("Counts", ORGS)
counts = Counts._make(
    [
        countContractByOrg(orgs, CATEGORIES, "contract_id", dfTp)
        for orgs in over_all(ORGS)
    ]
)


# %%
"""生成报表分析数据

根据违规类型按给定的组织机构进行统计分析. 分析结果:

组织机构 -> 违规类型 -> 违规合同数据量 -> 合同总量 -> 违规率 ->
                      上级机构的违规合同数据 -> 上级机构合同总量 -> 上级机构违规率

生成四份报表:

- 事业部-分公司维度
- 分公司-项目部维度
- 项目部-项目维度
- 分公司-项目维度
"""


@tz.curry
def genReport(
    dfCross: pd.DataFrame, childDf: pd.DataFrame, parentDf: pd.DataFrame
) -> pd.DataFrame:
    return (
        pd.merge(
            dfCross[childDf.index.names].drop_duplicates(),
            childDf.reset_index(),
            how="left",
            on=childDf.index.names,
        )
        .merge(parentDf, how="left", on=parentDf.index.names, suffixes=("", "_p"))
        .pipe(lambda df: df[df["irr_category"].notna()])
    )


# - branch: 事业部-分公司维度
# - dept: 分公司-项目部维度
# - project: 项目部-项目维度
# - prj: 分公司-项目维度
Reports = namedtuple("Reports", ["branch", "dept", "project", "prj"])
reports = Reports(
    branch=genReport(dfIrrTypeWithPrj, counts.branch, counts.division).fillna(0),
    dept=genReport(dfIrrTypeWithPrj, counts.dept, counts.branch).fillna(0),
    project=genReport(dfIrrTypeWithPrj, counts.project, counts.dept).fillna(0),
    prj=genReport(dfIrrTypeWithPrj, counts.project, counts.branch).fillna(0),
)


# %%
"""本期新增不规范合同报表
"""

rptIncrease = (
    dfIncrease.groupby(by=["branch", "irr_category"])[["contract_id"]]
    .count()
    .unstack()
    .droplevel(0, axis=1)
    .fillna(0)
    .assign(
        RN=lambda df: df["RN"] if contains(df.columns, "RN") else 0,
        TN=lambda df: df["TN"] if contains(df.columns, "TN") else 0,
        SN=lambda df: df["SN"] if contains(df.columns, "SN") else 0,
        total=lambda df: df.sum(axis=1),
        grand_total=lambda df: df["total"].sum(),
        proportion=lambda df: round(df["total"] / df["grand_total"], 4),
    )
    .rename_axis(None, axis=1)
    .reset_index()
)

# %%
"""事业部各分公司各种不符合规范操作合同的情况以及和事业部平均数据的对比情况
"""

anlOrg = reports.branch[
    tz.pipe(reports.branch.columns, tz.remove(partial(eq, "division")), list)
].sort_values(by=["branch", "irr_category"])

# %%
"""事业部各分公司综合情况分析
"""

anlBranch = (
    reports.branch[["branch", "irr_category", "rate"]]
    .fillna(0)
    .pivot(index="branch", columns="irr_category")
    .droplevel(0, axis=1)
    .assign(sum_up=lambda df: df.mean(axis=1))
    .sort_values(by="sum_up", ascending=False)
    .rename_axis(None, axis=1)
    .reset_index()
)

# %%
# typing config (json)

Column = TypedDict(
    "Column",
    {"name": str, "dict": Optional[Dict[str, str]], "formatter": Optional[str]},
)
Columns = Dict[str, Column]

Sort = TypedDict("Sort", {"ascending": bool, "value": List[str]})

Sheet = TypedDict(
    "Sheet",
    {
        "sheetName": str,
        "columns": Optional[Columns],
        "export": List[str],
        "sort": Sort,
    },
)
Sheets = Dict[str, Sheet]

ExcelFile = TypedDict("ExcelFile", {"name": str, "sheets": Dict[str, Sheet]})

Config = TypedDict("Config", {"columns": Columns, "ExcelFiles": Dict[str, ExcelFile]})

# %%
# help function for config (json)

# getExcelFile :: Config -> str -> ExcelFile
def getExcelFile(excelFileKey):
    return tz.get_in(["ExcelFiles", excelFileKey])


# getSheet :: ExcelFile -> str -> Sheet
def getSheet(sheetKey):
    return tz.get_in(["sheets", sheetKey])


# getColumns :: Sheet | Config -> Columns
def getColumns(d):
    v = tz.get_in(["columns"], d)
    return v if v else {}


# getColumn :: Columns -> str -> Column
def getColumn(columnKey):
    return tz.get_in([columnKey], default={})


@tz.curry
def getRealColumn(columnKey, sheetKey, excelFileKey, cfg):
    return tz.pipe(
        cfg,
        tz.juxt(
            tz.compose(getColumn(columnKey), getColumns),
            tz.compose(
                getColumn(columnKey),
                getColumns,
                getSheet(sheetKey),
                getExcelFile(excelFileKey),
            ),
        ),
        tz.merge,
    )


# Testing
# =======

# Be lazy and test with actual data

assert getRealColumn("branch", "irrAll", "issue", config) == {
    "name": "分公司",
    "dict": None,
    "formatter": None,
}

assert getRealColumn("irr_p", "rptBranch", "issue", config) == {
    "name": "事业部不合规范合同数量",
    "dict": None,
    "formatter": None,
}


assert getRealColumn("over_type", "rptBranch", "issue", config) == {
    "name": "合同终止类型",
    "dict": {
        "1": "正常终止",
        "2": "提前终止",
        "3": "续签终止",
        "4": "延后终止",
        "5": "变更终止",
        "6": "更名终止",
        "7": "合同解约",
    },
    "formatter": None,
}

assert getRealColumn("rate", "anlOrg", "analysis", config) == {
    "name": "不合规范合同比率",
    "dict": None,
    "formatter": "{:.2%}",
}

# %%
# helper function for style
# =========================


@tz.curry
def highlightRow(
    predicate: Callable[[pd.Series, pd.DataFrame], bool], props: str, df: pd.DataFrame,
) -> pd.DataFrame:
    return df.apply(
        lambda currentRow: np.where(predicate(currentRow, df), props, None),
        # axis=1: apply function to row
        axis=1,
        # result_type="broadcast": return DataFrame
        result_type="broadcast",
    )


# AR: Account Receivable
# columnName -> (pd.Serial, pd.DataFrame) -> bool
@tz.curry
def containsSeries(contained, columnName):
    return lambda currentRow, _: contained in currentRow[columnName]


# children -> parent -> (pd.Serial, pd.DataFrame) -> bool
@tz.curry
def greaterThan(leftColumnName, rightColumnName):
    return (
        lambda currentRow, _: currentRow[leftColumnName] > currentRow[rightColumnName]
    )


# columnName -> (pd.Serial, pd.DataFrame) -> bool
@tz.curry
def eqMax(columnName):
    return lambda currentRow, df: currentRow[columnName] == df[columnName].max()


@tz.curry
def containStyler(contained, columnName, renameColumns):
    return highlightRow(
        containsSeries(contained, renameColumns[columnName]),
        "background-color: lightpink",
    )


@tz.curry
def greaterThanStyler(leftColumnName, rightColumnName, renameColumns):
    return highlightRow(
        greaterThan(renameColumns[leftColumnName], renameColumns[rightColumnName]),
        "background-color: lightpink",
    )


@tz.curry
def eqMaxStyler(columnName, renameColumns):
    return highlightRow(
        eqMax(renameColumns[columnName]), "background-color: lightpink",
    )


# %%
# help function for export Excel file
# ###################################


@tz.curry
def toExcel(
    outDir: Path,
    excelFileKey: str,
    cfg: Config,
    iter_: Iterator[Tuple[str, pd.DataFrame]],
):
    excelFile: ExcelFile = getExcelFile(excelFileKey)(cfg)
    with pd.ExcelWriter(outDir / excelFile["name"]) as writer:
        for sheetKey, df, styler in iter_:
            sheet: Sheet = getSheet(sheetKey)(excelFile)
            export = sheet["export"]
            sort = sheet["sort"]
            columns = tz.merge(getColumns(cfg), getColumns(sheet))
            remapColumns = tz.pipe(
                columns,
                tz.keyfilter(partial(contains, export)),
                tz.valfilter(tz.get("dict")),
                tz.valmap(tz.get("dict")),
            )
            renameColumns = tz.pipe(
                columns,
                tz.keyfilter(partial(contains, export)),
                tz.valmap(tz.get("name")),
            )
            formatters = tz.pipe(
                columns,
                tz.keyfilter(partial(contains, export)),
                tz.valfilter(tz.get("formatter")),
                methodcaller("values"),
                tz.map(lambda d: (d["name"], d["formatter"])),
                dict,
            )

            (
                df[export]
                .replace(remapColumns)
                .sort_values(by=sort["value"], ascending=sort["ascending"])
                .rename(columns=renameColumns)
                .pipe(lambda df: df.set_index(np.arange(1, len(df) + 1)))
                .style.apply(styler(renameColumns), axis=None)
                .format(formatter=formatters)
                .to_excel(writer, sheet_name=sheet["sheetName"],)
            )


# %%
# 导出下发数据
#
# 包括:
#
# - 全量不合规合同清单
# - 增量不合规合同清单
# - 分公司维度统计报表
# - 项目部维度统计报表
# - 项目维度统计报表
# - 分公司+项目维度统计报表
# =======================

issueTuple = zip(
    [
        "irrAll",
        "irrIncrease",
        "rptBranch",
        "rptDept",
        "rptProject",
        "rptPrj",
        "rptIncrease",
    ],
    [
        dfIrr,
        dfIncrease,
        reports.branch,
        reports.dept,
        reports.project,
        reports.prj,
        rptIncrease,
    ],
    [
        containStyler("应收", "reason"),
        containStyler("应收", "reason"),
        greaterThanStyler("rate", "rate_p"),
        greaterThanStyler("rate", "rate_p"),
        greaterThanStyler("rate", "rate_p"),
        greaterThanStyler("rate", "rate_p"),
        eqMaxStyler("proportion"),
    ],
)

toExcel(paths.out, "issue", config, issueTuple)

# 导出分析文件
# ===========

analysisTuple = zip(
    ["anlOrg", "anlBranch"],
    [anlOrg, anlBranch],
    [greaterThanStyler("rate", "rate_p"), eqMaxStyler("sum_up")],
)

toExcel(paths.out, "analysis", config, analysisTuple)
