import pprint

import pandas as pd

def transform_column_dtypes(df):
    """
    convert a boolean-like columns to bit as in its corresponding column.
    Will transform the values in the provided column to 0 or 1 to match the column setup in db
    :param df: dataframe whose columns should be transformed
    :return: dataframe with the columns transformed
    """

    for col in list(df.columns):
        df[col].replace({'True': 1, 'False': 0}, inplace=True)

    """
    Changes the columns whose data types  are loaded as object in the dataframe to string.
    This should be extended to accept a dictionary (or something like that) whose key is the current dtype and value is the desired dtype
    """
    for col in df.columns:
        if df.dtypes[col] == 'object':
            df[col] = df[col].astype("string")
    print(df.dtypes)
    return df


def transform_date_columns(df):
    """
    Undefined dates are represented as NaT in pandas. change these to Null
    :param df:
    :return: dataframe with processed dates
    """

    for col in df.columns:
        if df[col].dtype == 'datetime64[ns]':
            df = df.applymap(lambda x: None if pd.isna(x) else x)

    pprint.pprint(df.columns)
    return df

