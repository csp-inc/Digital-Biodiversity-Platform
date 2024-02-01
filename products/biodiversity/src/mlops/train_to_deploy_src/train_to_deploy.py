# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import argparse
import logging
import os
import sys
import time
from uuid import uuid4

import mlflow
from azure_logger import AzureLogger
from lightgbm import LGBMClassifier
from utils.read_utils import read_from_config_file, read_from_csv
from utils.write_utils import write_to_json


def main(
    prep_data_path,
    best_parameters_path,
    model_path,
    model_metadata,
    correlation_id,
):
    if correlation_id is None:
        correlation_id = uuid4()

    azure_logger = AzureLogger(correlation_id=correlation_id, level=logging.DEBUG)

    start_time = time.time()
    try:
        features_path = os.path.join(prep_data_path, "features_data.csv")
        features_dataframe = read_from_csv(
            data_file_path=features_path, index_column=0, azure_logger=azure_logger
        )

        targets_path = os.path.join(prep_data_path, "targets_data.csv")
        targets_dataframe = read_from_csv(
            data_file_path=targets_path, index_column=0, azure_logger=azure_logger
        )

        best_parameter_space_path = os.path.join(
            best_parameters_path, "best_parameters.yml"
        )
        best_parameter_space = read_from_config_file(
            best_parameter_space_path, azure_logger
        )

        full_model = LGBMClassifier(**best_parameter_space)
        full_model.fit(features_dataframe, targets_dataframe, verbose=0)

        modelinf = mlflow.lightgbm.log_model(full_model, "lgbm_gbif_model")

        model_data = {"run_id": modelinf.run_id, "run_uri": modelinf.model_uri}
        write_to_json(model_metadata, model_data, azure_logger)

        mlflow.lightgbm.save_model(full_model, model_path)

    except BaseException as ex:
        azure_logger.exception(ex, __name__)
        sys.exit(1)
    finally:
        end_time = time.time()

        azure_logger.event(
            "completed train_to_deploy.py",
            **{"duration (s)": str((end_time - start_time))},
        )


def __define_arguments(args):
    parser = argparse.ArgumentParser("train_to_deploy")

    parser.add_argument(
        "--prep_data_path",
        type=str,
        required=True,
        help="Path to the folder containing the features and targets for training",
    )

    parser.add_argument(
        "--best_parameters_path",
        type=str,
        required=True,
        help="Path to the file containing the parameters for the model",
    )

    parser.add_argument(
        "--model_output_path",
        type=str,
        required=False,
        help="Path to the folder where the final trained model will be written",
    )

    parser.add_argument(
        "--model_metadata",
        type=str,
        required=False,
        help="Path to the file where model metadata will be written",
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
        args.prep_data_path,
        args.best_parameters_path,
        args.model_output_path,
        args.model_metadata,
        args.correlation_id,
    )
