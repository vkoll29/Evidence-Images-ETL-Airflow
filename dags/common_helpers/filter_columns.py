def filter_columns(df, columns_to_keep: list):
    """
       this function takes a dataframe and a list of columns to keep and returns a df only with those specified columns
       :param columns_to_keep: list of columns that will be kept for the dataframe
       :param df: larger dataframe that includes unneeded columns
       :return: minimized df with only the specified columns
       """
    print(df.columns)
    for col in df.columns:
        if col.lower() not in [col_keep.lower() for col_keep in columns_to_keep]:
            del df[col]

    print(df.columns)
    return df