# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

from typing import Callable, Tuple

import mlflow
from azure_logger import AzureLogger
from hyperopt import Trials, atpe, tpe
from hyperopt.fmin import fmin
from models.lgbm import get_parameter_space_lgbm
from utils.read_utils import read_from_config_file


def run_hyperoptimization(
    param_space: dict,
    objective: Callable,
    algo: Callable = tpe,
    trials: Trials = None,
    max_evals: int = 100,
) -> Tuple[dict, Trials]:
    """
    Runs hyperopt hyperparameter optimization cycle

    Parameters
    ----------
    param_space : dict
        Dictionary of parameters and hyperparameters
    objective : Callable
        Objective function to be used for hyperopt hyperparameter tuning
    algo: Callable
        Hyperparameter space search algorithm.
        Options: tpe / (atpe) adaptive tpe search, rand: random search
    trials : Trials
        Hyperopt trials object. Can be reused between multiple runs, by default None
    max_evals : int
        Number of hyperparam iterations

    Returns
    -------
    Tuple[dict, Trials]
        Best results dictionary and Trials object
    """
    if trials is None:
        trials = Trials()
    best = fmin(
        fn=objective,
        space=param_space,
        algo=algo.suggest,
        max_evals=max_evals,
        trials=trials,
    )
    return best, trials


def parse_parameter_space_lgbm(
    parameter_file_path: str, azure_logger: AzureLogger
) -> Tuple[dict, dict]:
    """
    Read the parameters from a .yml file and obtains the
    specific parameters of the LGBM model.

    Parameters
    ----------
    parameter_file_path : dict
        Path of the .yml file that contains the parameters.
    azure_logger: AzureLogger
        AzureLogger instance that communicates with AppInsights.

    Returns
    -------
    parameters : dict
        Initial parameters for the LGBM algorithm.
    parameters_to_tune : dict
        List of parameters the hyperoptimization algorithm
        will tune for.
    """

    parameters = read_from_config_file(parameter_file_path, azure_logger)
    parameters_to_tune = get_parameter_space_lgbm()

    return parameters, parameters_to_tune


def hyperparameter_algorithm_parsing(algorithm: str, azure_logger: AzureLogger):
    """
    Assigns the correct algorithm based on the input string.

    Parameters
    ----------
    algorithm : str
        Name of the algorithm.
    azure_logger: AzureLogger
        AzureLogger instance that communicates with AppInsights.

    Returns
    -------
    Union[tpe, atpe]
        Algorithm object
    """

    if algorithm == "tpe":
        return tpe
    elif algorithm == "atpe":
        return atpe

    message = "Invalid hyperoptimization algorithm"
    azure_logger.event(message, **{message: algorithm})
    raise Exception(message + " " + algorithm)


def trials_mlflow_logging(trials: Trials) -> None:
    """
    Logs all the results from the hyperparameter tuning process
    in MLFLow.

    Parameters
    ----------
    trials : Trials
        The trials ran by the hyperoptimization algorithm.

    Returns
    -------
    None
    """
    for trial in trials.trials:
        loss = trial["result"]["loss"]
        parameters = trial["misc"]["vals"]

        # Convert values from lists to integers
        for key, value in parameters.items():
            parameters[key] = value[0]

        mlflow.log_metric("loss", loss)
        mlflow.log_metrics(parameters)
