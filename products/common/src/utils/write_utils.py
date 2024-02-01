import json
import pickle

import numpy as np
import pandas as pd
from azure_logger import AzureLogger
from sklearn.base import BaseEstimator


def convert_to_json(dictionary: dict) -> str:
    """
    Converts a dictionary to json format.

    Parameters
    ----------
    dictionary : dict
        The dictionary to be converted in json format.

    Returns
    -------
        Formatted dictionary in json, appears as string.
    """
    json_object = json.dumps(dictionary, indent=4)

    return json_object


def write_to_json(output_file_path: str, data: dict, azure_logger: AzureLogger) -> None:
    """
    Writes data to a json file.

    Parameters
    ----------
    output_file_path : str
        Path to file the data will be written to.
    data: str
        Data in dictionary format.
    azure_logger: AzureLogger
        AzureLogger instance that communicates with AppInsights.

    Returns
    -------
        None.
    """

    with open(output_file_path, "w") as json_file:
        json.dump(data, json_file, indent=4)

    azure_logger.log("write", **{"json file": f"{output_file_path}"})


def write_model_to_pickle(
    model: BaseEstimator, output_file_path: str, azure_logger: AzureLogger
) -> None:
    """
    Writes model to a pickle file.

    Parameters
    ----------
    model: BaseEstimator
        Trained sklearn model.
    output_file_path : str
        Path to the file the model will be written to.
    azure_logger: AzureLogger
        AzureLogger instance that communicates with AppInsights.

    Returns
    -------
        None.
    """
    with open(output_file_path, "wb") as model_file:
        pickle.dump(model, model_file)

    azure_logger.log("write", **{"json file": f"{output_file_path}"})


def write_to_csv(
    data: pd.DataFrame,
    output_file_path: str,
    azure_logger: AzureLogger,
) -> None:
    """
    Writes data to a csv file.

    Parameters
    ----------
    data: pd.DataFrame
        Data stored in a pandas dataframe format.
    output_file_path: str
        Csv file location for the data to be written to.
    azure_logger: AzureLogger
        AzureLogger instance that communicates with AppInsights.

    Returns
    -------
        None.
    """

    data.to_csv(output_file_path)

    azure_logger.log("write", **{"Csv file": f"{output_file_path}"})


def convert_dictionary_numpy_values_to_json_readable(dictionary: dict) -> dict:
    """
    Converts the numpy values of a dictionary from numpy values
    to Python standard ints and floats.

    Parameters
    ----------
    dictionary : dict
        The dictionary to be converted.

    Returns
    -------
        Dictionary with json readable values.
    """

    for key, val in dictionary.items():
        if isinstance(val, np.integer):
            dictionary[key] = int(val)
        elif isinstance(val, np.floating):
            dictionary[key] = float(val)

    return dictionary
