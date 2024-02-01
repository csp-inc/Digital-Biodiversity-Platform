# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import logging
import uuid

import pandas as pd
from azure_logger import AzureLogger


def combine_data_tables(  # noqa: C901
    dfs,
    drop_duplicates: bool = False,
    handle_nans="drop",
    fill_value=0,
    rename_cols=None,
    id_col: str = "gbifid",
    correlation_id: uuid.UUID = uuid.UUID(int=0),
) -> pd.DataFrame:
    """
    Combines multiple feature dataframes to one dataframe.

    Parameters
    ----------
    dfs : iterable of pd.DataFrames
        The individual feature dataframes to combine. Expects each dataframe
        to have a column matching the input parameter: `id_col`.
    drop_duplicates : bool; default=False
        If True, drops duplicate rows from each dataframe in dfs based on ID
        column, keeping the last entry encountered.
    handle_nans : str; default='drop'
        Determines handling of NaNs in the merged dataframe. Options:
        'drop': drops all rows that contain any NaNs;
        'fill': fills NaNs according to fill_value.
    fill_value : int/float or str; default=0
        Determines how NaNs are filled. Options:
        1) a specific value (int/float);
        2) 'mean'/'median' to fill with column-wise mean or median values.
    rename_cols : dict; default=None
        A dictionary mapping old to new column names.
    verbose : bool; default=False
        If True, prints messages about process.
    id_col : str; default='gbifid'
        Column name of ID column in input dataframes

    Returns
    -------
    pd.DataFrame
        The combined dataframe.
    """
    azure_logger = AzureLogger(correlation_id=correlation_id, level=logging.DEBUG)

    # get IDs present in each of the dataframes
    # and set index to ID for joining
    all_ids = []
    for i in range(len(dfs)):
        df = dfs[i]
        all_ids.append(list(df[id_col]))
        df = df.set_index(id_col, drop=False)
        if drop_duplicates:
            df = df.drop_duplicates(subset=id_col, keep="last")
        dfs[i] = df

    # dropping rows that are not present in all dataframes;
    # this is done before joining to ensure there are no data gaps
    # affecting modeling results
    azure_logger.log("Selecting common rows.")

    keep_rows = list(set(all_ids[0]))
    for i in range(1, len(all_ids), 1):
        keep_rows = list(set(keep_rows).intersection(all_ids[i]))

    dfs = [df.loc[keep_rows] for df in dfs]

    azure_logger.log("Initial dataframe rows selected", **{"length": len(keep_rows)})
    azure_logger.log("Combining dataframes.")

    df_full = pd.concat(dfs, axis=1)
    df_full = df_full.drop(id_col, axis=1)

    if handle_nans == "fill":
        azure_logger.log("Filling NaNs", **{"fill_value": fill_value})
        if isinstance(fill_value, float) or isinstance(fill_value, int):
            df_full = df_full.fillna(fill_value)
        elif fill_value == "mean":
            df_full = df_full.fillna(df_full.mean())
        elif fill_value == "median":
            df_full = df_full.fillna(df_full.median())
    elif handle_nans == "drop":
        azure_logger.log("Dropping rows containing NaNs.")
        df_full = df_full.dropna(axis=0, how="any")
    azure_logger.log("Final dataframe rows selected", **{"length": len(keep_rows)})

    if rename_cols is not None:
        azure_logger.log("Renaming columns.")
        df_full = df_full.rename(rename_cols, axis=1)

    return df_full
