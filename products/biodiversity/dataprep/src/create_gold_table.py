# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import argparse
import json
import logging
import os
import sys
import time
import uuid
from os import path

import pandas as pd
import utils.key_vault_utils as key_vault_utils
from argument_exception import ArgumentException
from azure_logger import AzureLogger
from datasets.gold_dataset import combine_data_tables


def create_gold_data_table(
    gbif_extension: str,
    drop_duplicates=True,
    handle_nans: str = "drop",
    fill_value=0,
    rename_cols=None,
    gbif_feature_dir="./",
    csv_output_path="./",
    correlation_id: uuid.UUID = uuid.uuid4(),
) -> None:
    """
    Creates the gold data table by reading and combining a
    number of input data files.

    Parameters
    ----------
    gbif_feature_files: list[str]
        List of files names that hold the input data sets. They must all exist in
        the gbif_features_dir directory.
    drop_duplicates : Bool
        If True, drops duplicate rows from each dataframe in dfs based on ID
        column, keeping the last entry encountered
    handle_nans: str
        Determines handling of NaNs in the merged dataframe. One of 'drop' or 'fill'
    fill_value: Any
        Fills NaNs with the specific value. Either an integer/float value to use
        or 'mean' or 'median'.
    rename_cols: dict[str, str]
        A dictionary mapping old to new column names.
    gbif_feature_dir: str
        Local directory holding the GBIF feature files.
    csv_output_path: str
        Path and filename for the output csv file
    correlation_id: uuid
        Optional Application Insights correlation id.
    Returns
    -------
        None
    """
    azure_logger = AzureLogger(correlation_id=correlation_id, level=logging.DEBUG)

    feature_dfs = []

    if not path.exists(gbif_feature_dir):
        raise FileNotFoundError("GBIF feature dir not found.")

    for feature, dirs, files in os.walk(gbif_feature_dir):
        # Aggregate all CSV files for one feature
        azure_logger.log("Loading feature", **{"feature:": feature})
        one_feature_dfs = []
        for file in [f for f in files if f.endswith(gbif_extension)]:
            # Create list of dataframes, one per file
            file_path = os.path.join(feature, file)
            azure_logger.log("Loading file", **{"file_path:": file_path})
            df = pd.read_csv(file_path)
            one_feature_dfs.append(df)

        # Concatenate all dataframes for one feature into a single dataframe
        if len(one_feature_dfs) > 0:
            feature_dfs.append(pd.concat(one_feature_dfs))

    azure_logger.log("Combining data sets")
    df_gold = combine_data_tables(
        feature_dfs,
        drop_duplicates=drop_duplicates,
        handle_nans=handle_nans,
        fill_value=fill_value,
        rename_cols=rename_cols,
    )

    azure_logger.log("Writing combined data", **{"output path:": csv_output_path})
    df_gold.to_csv(csv_output_path)


def main(argv):
    args = __define_arguments(argv)

    if args.key_vault_name is not None:
        key_vault_utils.get_environment_from_kv(args.key_vault_name)

    azure_logger = AzureLogger(correlation_id=args.correlation_id, level=logging.DEBUG)

    azure_logger.event("starting create_gold_table")

    start_time = time.time()

    try:
        fill_arg = args.fill_value
        if fill_arg is None and args.fill_kind is not None:
            fill_arg = args.fill_kind

        create_gold_data_table(
            gbif_extension=args.gbif_feature_files_extension,
            rename_cols=args.rename_cols,
            handle_nans=args.handle_nans,
            fill_value=args.fill_value,
            drop_duplicates=args.drop_duplicates,
            gbif_feature_dir=args.gbif_feature_dir,
            csv_output_path=args.csv_output_path,
            correlation_id=args.correlation_id,
        )
    except ArgumentException:
        sys.exit(2)
    except Exception as ex:
        azure_logger.exception(ex, "create_gold_table.py failed")
        sys.exit(1)
    finally:
        end_time = time.time()

        azure_logger.event(
            "completed create_gold_table",
            **{"duration (s)": str((end_time - start_time))},
        )


def __define_arguments(args) -> argparse.Namespace:
    """

    Parses the command line arguments.

    Parameters
    ----------
    args: list[str]
        Command line arguments presented as a list of strings.

    Returns:
    --------
    argsparse.Namespace
        Parsed arguments as a Namespace.

    """
    parser = argparse.ArgumentParser("create_gold_table")

    parser.add_argument(
        "--gbif_feature_files_extension",
        type=str,
        required=False,
        default=".csv",
        help="Extension of file names that hold the gbif features.",
    )

    parser.add_argument(
        "--rename_cols",
        type=json.loads,
        required=False,
        help="A json serailized dictionary mapping old to new column names.",
    )

    parser.add_argument(
        "--handle_nans",
        type=str,
        required=False,
        help="Determines handling of NaNs in the merged dataframe.",
        choices=("drop", "fill"),
    )

    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--fill_value",
        required=False,
        type=float,
        help="Fills NaNs with the specific value.",
    )

    group.add_argument(
        "--fill_kind",
        required=False,
        choices=("mean", "median"),
        help="Determines how NaNs are filled",
    )

    parser.add_argument(
        "--drop_duplicates",
        type=bool,
        required=True,
        help="If True, drops duplicate rows from each dataframe in dfs based on ID"
        "column, keeping the last entry encountered",
    )

    parser.add_argument(
        "--gbif_feature_dir",
        required=True,
        type=str,
        help="Local directory holding the GBIF feature files.",
    )

    parser.add_argument(
        "--csv_output_path",
        type=str,
        required=True,
        help="Path and filename for the output csv file",
    )

    parser.add_argument(
        "--correlation_id",
        type=str,
        required=False,
        default=uuid.uuid4(),
        help="Application Insights correlation id if required.",
    )

    parser.add_argument(
        "--key_vault_name",
        type=str,
        required=False,
        help="Key Vault name where to retrieve secrets.",
    )

    return parser.parse_args(args)


if __name__ == "__main__":
    main(sys.argv[1:])
