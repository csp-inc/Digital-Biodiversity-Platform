# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import argparse
import logging
import os
import sys
import time
from functools import partial
from uuid import uuid4

from azure_logger import AzureLogger
from models.lgbm import objective
from models.parameters import (
    hyperparameter_algorithm_parsing,
    parse_parameter_space_lgbm,
    run_hyperoptimization,
    trials_mlflow_logging,
)
from utils.data_utils import split_train_test
from utils.read_utils import read_from_csv
from utils.write_utils import (
    convert_dictionary_numpy_values_to_json_readable,
    write_to_csv,
    write_to_json,
)


def main(
    input_path,
    parameters_path,
    best_parameters_path,
    scoring_data_path,
    train_fraction,
    random_seed,
    number_of_folds,
    maximum_evaluations,
    hyperparameter_algorithm,
    correlation_id,
    verbose,
):
    if correlation_id is None:
        correlation_id = uuid4()

    azure_logger = AzureLogger(correlation_id=correlation_id, level=logging.DEBUG)

    start_time = time.time()
    try:
        features_path = os.path.join(input_path, "features_data.csv")
        features_dataframe = read_from_csv(
            data_file_path=features_path, index_column=0, azure_logger=azure_logger
        )

        targets_path = os.path.join(input_path, "targets_data.csv")
        targets_dataframe = read_from_csv(
            data_file_path=targets_path, index_column=0, azure_logger=azure_logger
        )

        features_train, features_test, targets_train, targets_test = split_train_test(
            features_dataframe=features_dataframe,
            targets_dataframe=targets_dataframe,
            train_fraction=train_fraction,
            random_seed=random_seed,
        )

        parameters, params_to_tune = parse_parameter_space_lgbm(
            parameters_path, azure_logger
        )
        parameter_space = {**parameters, **params_to_tune}

        model_objective = partial(
            objective,
            features=features_train,
            targets=targets_train,
            kfold=int(number_of_folds),
            verbose=int(verbose),
        )

        hyperparameter_algorithm = hyperparameter_algorithm_parsing(
            hyperparameter_algorithm, azure_logger
        )

        best_results, trials = run_hyperoptimization(
            param_space=parameter_space,
            objective=model_objective,
            algo=hyperparameter_algorithm,
            max_evals=int(maximum_evaluations),
        )

        trials_mlflow_logging(trials)

        best_parameter_space = {**parameters, **best_results}

        best_parameter_space.pop("early_stopping_rounds")

        best_parameter_space = convert_dictionary_numpy_values_to_json_readable(
            best_parameter_space
        )

        write_to_json(
            output_file_path=os.path.join(best_parameters_path, "best_parameters.yml"),
            data=best_parameter_space,
            azure_logger=azure_logger,
        )
        write_to_csv(
            output_file_path=os.path.join(scoring_data_path, "training_features_data.csv"),
            data=features_train,
            azure_logger=azure_logger,
        )
        write_to_csv(
            output_file_path=os.path.join(scoring_data_path, "training_targets_data.csv"),
            data=targets_train,
            azure_logger=azure_logger,
        )
        write_to_csv(
            output_file_path=os.path.join(scoring_data_path, "testing_features_data.csv"),
            data=features_test,
            azure_logger=azure_logger,
        )
        write_to_csv(
            output_file_path=os.path.join(scoring_data_path, "testing_targets_data.csv"),
            data=targets_test,
            azure_logger=azure_logger,
        )

    except BaseException as ex:
        azure_logger.exception(ex, __name__)
        sys.exit(1)
    finally:
        end_time = time.time()

        azure_logger.event(
            "completed train.py",
            **{"duration (s)": str((end_time - start_time))},
        )


def __define_arguments(args):
    parser = argparse.ArgumentParser("train")

    parser.add_argument(
        "--input_path",
        type=str,
        required=True,
        help="Path to the folder containing the features and targets for training",
    )

    parser.add_argument(
        "--parameters_path",
        type=str,
        required=True,
        help="Path to the file containing the parameters for the model",
    )

    parser.add_argument(
        "--best_parameters_path",
        type=str,
        required=False,
        help="Path to the folder where the optimal parameters will be written",
    )

    parser.add_argument(
        "--scoring_data_path",
        type=str,
        required=False,
        help="Path to the folder where the scoring features and "
        "targets datasets will be written",
    )

    parser.add_argument(
        "--train_fraction",
        type=float,
        required=False,
        default=0.8,
        help="Fraction of the data that will be used as training set",
    )

    parser.add_argument(
        "--random_seed",
        type=int,
        required=False,
        default=42,
        help="Seed used for the data split",
    )

    parser.add_argument(
        "--number_of_folds",
        type=int,
        required=False,
        default=5,
        help="Number of folds used for cross validation",
    )

    parser.add_argument(
        "--maximum_evaluations",
        type=int,
        required=False,
        default=1000,
        help="Maximum number of evaluations to be used for hyperparameter optimization",
    )

    parser.add_argument(
        "--hyperparameter_algorithm",
        type=str,
        required=False,
        default="tpe",
        help="Algorithm to be used for hyperparameter optimization",
    )

    parser.add_argument(
        "--correlation_id",
        type=str,
        required=False,
        help="Application Insights correlation id if required.",
    )

    parser.add_argument(
        "--verbose",
        type=float,
        required=False,
        default=-1,
        help="Verbosity level, by default -1.",
    )

    return parser.parse_args(args)


if __name__ == "__main__":
    args = __define_arguments(sys.argv[1:])

    main(
        args.input_path,
        args.parameters_path,
        args.best_parameters_path,
        args.scoring_data_path,
        args.train_fraction,
        args.random_seed,
        args.number_of_folds,
        args.maximum_evaluations,
        args.hyperparameter_algorithm,
        args.correlation_id,
        args.verbose,
    )
