# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import argparse
import logging
import sys
import time
import uuid

import pandas as pd
import utils.key_vault_utils as key_vault_utils
from argument_exception import ArgumentException
from azure_logger import AzureLogger


def merge_data(
    cherry_pt_segments: str,
    gold_table_path: str,
    batch_input_path: str,
    merged_df_path: str,
    azure_logger: AzureLogger = AzureLogger(
        correlation_id=uuid.uuid4(), level=logging.DEBUG
    ),
) -> None:
    """
    Merges data from gold table and cherry point segments to use as input
    for inference.

    Parameters
    ----------
    cherry_pt_segments: str
        Name of the cherry points segments file to use for inference.
    gold_table_path: str
        Path to the gold table csv file to use for inference.
    batch_input_path: str
        Path to output folder for merged data results.
    merged_df_path: str
        Path to the merged dataframe, used to compile inference results.
    azure_logger: AzureLogger
        Azure logger to use for logging.
    Returns
    -------
        None
    """

    # Load full gold dataset and cherry_pt segments
    gold_df = pd.read_csv(gold_table_path)
    cher_pt = pd.read_csv(cherry_pt_segments)

    # Prepare dataset for inference.
    merged_df = cher_pt.merge(gold_df, left_on="id", right_on="gbifid", how="left")

    # drop all 2013 rows because landsat/dswe/ndvi is not collected
    merged_df["year"] = merged_df.time.apply(lambda x: x.split("-")[0])
    merged_df = merged_df[~(merged_df["year"] == "2013")]

    # create input csv file
    columns_to_drop = [
        "soil_mukey_total",
        "id",
        "gbifid",
        "x",
        "y",
        "decimallatitude",
        "decimallongitude",
        "time",
        "year",
    ]

    merged_df.drop(merged_df.columns[0], axis=1, inplace=True)

    skinny_df = merged_df.drop(labels=columns_to_drop, axis=1).copy()
    skinny_df.to_csv(f"{batch_input_path}/batch_input.csv", index=False)

    merged_df.drop(columns=["gbifid"], axis=1, inplace=True)
    merged_df.to_csv(f"{merged_df_path}/merged_df.csv")


def main(argv):
    args = __define_arguments(argv)

    if args.key_vault_name is not None:
        key_vault_utils.get_environment_from_kv(args.key_vault_name)

    azure_logger = AzureLogger(correlation_id=args.correlation_id, level=logging.DEBUG)

    azure_logger.event("starting merge_data")

    start_time = time.time()

    try:
        merge_data(
            cherry_pt_segments=args.cherry_pt_segments,
            gold_table_path=args.gold_table_path,
            batch_input_path=args.batch_input_path,
            merged_df_path=args.merged_df_path,
            azure_logger=azure_logger,
        )
    except ArgumentException:
        sys.exit(2)
    except Exception as ex:
        azure_logger.exception(ex, "merge_data.py failed")
        sys.exit(1)
    finally:
        end_time = time.time()

        azure_logger.event(
            "completed merge_data",
            **{"duration (s)": str((end_time - start_time))},
        )


def __define_arguments(args):
    parser = argparse.ArgumentParser("merge_data")

    parser.add_argument(
        "--cherry_pt_segments",
        type=str,
        required=True,
        help="Path to the cherry points segments file to use for inference.",
    )

    parser.add_argument(
        "--gold_table_path",
        type=str,
        required=True,
        help="Path to the gold table csv file to use for inference.",
    )

    parser.add_argument(
        "--batch_input_path",
        type=str,
        required=True,
        help="Path to store the merged input to use for inference.",
    )

    parser.add_argument(
        "--merged_df_path",
        type=str,
        required=True,
        help="Path to store the merged dataframe to use to compile inference results.",
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
