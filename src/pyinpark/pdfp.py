from operator import methodcaller
from pathlib import Path
from typing import Any, Callable

import pandas as pd
import toolz.curried as tz

# cspell: disable
from pyinpark.pyfp import load_jsonc

# cspell: enable


@tz.curry
def create_df_from_json(
    data_key: str, columns_key: str, json_data: dict
) -> pd.DataFrame:
    return pd.DataFrame(json_data[data_key], columns=json_data[columns_key])


@tz.curry
def create_df_from_file(file_path: Path) -> pd.DataFrame:
    fn_dict = {".json": load_jsonc, ".xlsx": pd.read_excel, ".csv": pd.read_csv}
    fn = fn_dict[file_path.suffix]
    return fn(file_path)


@tz.curry
def df_to_file(filename: Path, method: str):
    return methodcaller(method, filename)


@tz.curry
def loc_by(
    filter_fn: Callable[[pd.DataFrame], pd.Series], df: pd.DataFrame
) -> pd.DataFrame:
    return df[filter_fn(df)]


@tz.curry
def isin_series(in_series: pd.Series) -> Callable[[pd.Series], pd.Series]:
    return methodcaller("isin", in_series)


@tz.curry
def between_series(left_ser: pd.Series, right_ser: pd.Series, v: Any) -> pd.Series:
    return (left_ser <= v) & (v < right_ser)


@tz.curry
def eq_series(ser: pd.Series, v: Any) -> pd.Series:
    return ser == v


@tz.curry
def get_value_by_booleans(
    default: Any, value_ser: pd.Series, booleans: pd.Series
) -> Any:
    return (
        value_ser.loc[booleans].values[0]
        if len(value_ser.loc[booleans].values)
        else default
    )


@tz.curry
def get_rowsValues(df):
    return [v.values for _, v in df.iterrows()]
