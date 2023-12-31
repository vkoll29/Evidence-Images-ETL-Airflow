import pandas as pd
def concat_dfs(dfs_list):
    """
    Each of the get_country_blobs task generates a df. these are then added to a list. This task is intended to concatenate the dfs into one

    :param dfs_list: list of dfs from each blob ingestion task
    :return: one whole dataframe
    """
    print(len(dfs_list))
    print(dfs_list)
    if len(dfs_list) > 0:
        df = pd.concat(dfs_list)
        print(len(df))
        return df
    else:
        return pd.DataFrame()