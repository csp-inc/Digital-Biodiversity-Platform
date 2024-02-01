import pickle
import re
from typing import Union

import pandas as pd
import yaml
from azure_logger import AzureLogger
from sklearn.base import BaseEstimator


def read_from_config_file(config_file_path: str, azure_logger: AzureLogger) -> dict:
    """
    Reads data from a configuration file.

    Parameters
    ----------
    fname : str
        Name of the file containing configuration.
    azure_logger: AzureLogger
        AzureLogger instance that communicates with AppInsights.

    Returns
    -------
    dict
        Contents of the file in a dictionary format.
    """

    with open(config_file_path, "r") as f:
        data = yaml.safe_load(f)

    azure_logger.log("read", **{"parameters": f"{config_file_path}"})

    return data


def load_model_from_pickle(
    model_file_path: str, azure_logger: AzureLogger
) -> BaseEstimator:
    """
    Loads model object from pickle file.

    Parameters
    ----------
    model_file_path : str
        Path to model.pkl file.
    azure_logger: AzureLogger
        AzureLogger instance that communicates with AppInsights.

    Returns
    -------
        BaseEstimator sklearn estimator, allows for all types of algorithms.
    """

    with open(model_file_path, "rb") as model_file:
        model = pickle.load(model_file)

    azure_logger.log("read", **{"model": f"{model_file_path}"})

    return model


def read_from_csv(
    data_file_path: str,
    azure_logger: AzureLogger,
    index_column: Union[int, str, None] = None,
) -> pd.DataFrame:
    """
    Reads data from a csv file.

    Parameters
    ----------
    data_file_path : str
        Path to file that contains the data.
    azure_logger: AzureLogger
        AzureLogger instance that communicates with AppInsights.
    index_column: int=0
        Column to be used as index.

    Returns
    -------
        Pandas dataframe.
    """

    data = pd.read_csv(filepath_or_buffer=data_file_path, index_col=index_column)

    # Removes special JSON characters
    data = data.rename(columns=lambda x: re.sub("[^A-Za-z0-9_]+", "", x))

    azure_logger.log("read", **{"data": f"{data_file_path}"})

    return data


def lower_replace(string: str, separator_to_replace: str, new_separator: str) -> str:
    """
    Removes trailing and leading whitespaces and replaces
    whitespaces with desired separator.

    Parameters
    ----------
    string : str
        String to be formatted.
    separator_to_replace: str
        Separator that needs replacing
    new_separator: str
        New separator of choice.

    Returns
    -------
        String.
    """

    return string.strip().lower().replace(separator_to_replace, new_separator)
