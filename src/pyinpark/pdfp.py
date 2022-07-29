import toolz.curried as tz
import pandas as pd


@tz.curry
def create_df_from_json(
    data_key: str, columns_key: str, json_data: dict
) -> pd.DataFrame:
    return pd.DataFrame(json_data[data_key], columns=json_data[columns_key])
