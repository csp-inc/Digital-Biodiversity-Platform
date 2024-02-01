# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Prep logic."""

import argparse
import logging
import os
import sys
import time
import uuid
from typing import Tuple

import pandas as pd
from azure_logger import AzureLogger
from utils.read_utils import read_from_csv
from utils.write_utils import write_to_csv


def main(
    raw_data_gbif_path,
    raw_data_species_path,
    species_file_name,
    prep_data_path,
    columns_to_drop,
    target_col_name,
    drop_nans,
    correlation_id,
):
    if correlation_id is None:
        correlation_id = uuid.uuid4()

    azure_logger = AzureLogger(correlation_id=correlation_id, level=logging.DEBUG)

    start_time = time.time()
    try:
        azure_logger.log("raw_data files")

        raw_data_gbif_dataframe = read_from_csv(raw_data_gbif_path, azure_logger)
        raw_data_species_dataframe = read_from_csv(
            os.path.join(raw_data_species_path, species_file_name), azure_logger
        )

        features_dataframe, targets_dataframe = prepare_dataset(
            gbif_df=raw_data_gbif_dataframe,
            species_df=raw_data_species_dataframe,
            columns_to_drop=columns_to_drop,
            target_col_name=target_col_name,
            drop_na=drop_nans,
        )

        features_file_path = os.path.join(prep_data_path, "features_data.csv")
        targets__file_path = os.path.join(prep_data_path, "targets_data.csv")

        write_to_csv(features_dataframe, features_file_path, azure_logger)
        write_to_csv(targets_dataframe, targets__file_path, azure_logger)

    except BaseException as ex:
        azure_logger.exception(ex, __name__)
        sys.exit(1)
    finally:
        end_time = time.time()
        azure_logger.event(
            "completed prep.py",
            **{"duration (s)": str((end_time - start_time))},
        )


def prepare_dataset(
    gbif_df: pd.DataFrame,
    species_df: pd.DataFrame,
    drop_na: bool,
    columns_to_drop: list,
    target_col_name: str,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Merges the raw data coming from gbif with species specific data.
    Preprocessing steps such as dropping null values and columns are applied.
    The function generates two separate files,
    one which contains the features for training and one the targets.
    Parameters
    ----------
    gbif_df: pd.DataFrame
        DataFrame containing the gbif data.
    species_df: pd.DataFrame,
        DataFrame containing the species data.
    drop_na: bool
        Boolean which indicates whether rows with null values should be dropped.
    columns_to_drop: list
        List of columns to be dropped from the merged DataFrame.
    target_col_name: str
        The name of the column that contains the target data.
    Returns
    -------
        features_dataframe: pd.DataFrame
            DataFrame containing the features.
        targets_dataframe: pd.DataFrame
            DataFrame containing the targets.
    """

    gbif_id_as_int = gbif_df["gbifid"].astype(float).astype(int)
    gbif_df["gbifid"] = gbif_id_as_int

    species_id_as_int = species_df["gbifid"].astype(float).astype(int)
    species_df["gbifid"] = species_id_as_int

    # Merge datasets
    merged_dataset = gbif_df.merge(
        species_df,
        left_on="gbifid",
        right_on="gbifid",
        how="inner",
        suffixes=("_all", "_species"),
    )

    # Drop rows with NaNs
    if drop_na:
        merged_dataset.dropna(inplace=True)

    # Drop columns
    merged_dataset.drop(columns=columns_to_drop, axis=1, inplace=True)

    # Drop target column
    features_dataframe = merged_dataset.drop(target_col_name, axis=1)

    # Create target variable
    targets_dataframe = merged_dataset[target_col_name]

    # Encode target variable
    target_encoding = {"PRESENT": 1, "ABSENT": 0}
    targets_dataframe = targets_dataframe.apply(lambda x: target_encoding[x])

    return (features_dataframe, targets_dataframe)


def __define_arguments(args):
    parser = argparse.ArgumentParser("prep")

    parser.add_argument(
        "--raw_data_gbif_path",
        type=str,
        required=True,
        help="Path to all species data",
    )

    parser.add_argument(
        "--raw_data_species_path",
        type=str,
        required=True,
        help="Path to specific species data",
    )

    parser.add_argument(
        "--species_file_name",
        type=str,
        required=True,
        help="Name of the species CSV file",
    )

    parser.add_argument(
        "--prep_data_path",
        type=str,
        required=False,
        help="Path to output folder where the targets and features will be stored",
    )

    parser.add_argument(
        "--columns_to_drop",
        nargs="*",
        type=str,
        required=False,
        help="A list of columns to be dropped, this would be passed to the cli"
        "as a succesion of strings: 'soil_mukey_total' 'gbifid' 'decimallatitude'",
    )

    parser.add_argument(
        "--target_col_name",
        type=str,
        required=False,
        help="The column to be used as target.",
    )

    parser.add_argument(
        "--drop_nans",
        type=bool,
        required=False,
        help="If True, drops duplicate rows from each dataframe in dfs based on ID"
        "column, keeping the last entry encountered",
    )

    parser.add_argument(
        "--correlation_id",
        type=str,
        required=False,
        help="Application Insights correlation id if required.",
    )

    return parser.parse_args(args)


if __name__ == "__main__":
    args = __define_arguments(sys.argv[1:])

    main(
        args.raw_data_gbif_path,
        args.raw_data_species_path,
        args.species_file_name,
        args.prep_data_path,
        args.columns_to_drop,
        args.target_col_name,
        args.drop_nans,
        args.correlation_id,
    )
