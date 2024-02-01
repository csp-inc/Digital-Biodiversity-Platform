# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import logging
import os
from uuid import uuid4

import mlflow
import pandas as pd
from azure_logger import AzureLogger
from utils.read_utils import read_from_csv


def init():
    global model

    # AZUREML_MODEL_DIR is an environment variable created during deployment
    # It is the path to the model folder
    model_path = os.path.join(os.environ["AZUREML_MODEL_DIR"], "lgbm_gbif_model")
    model = mlflow.lightgbm.load_model(model_path)


def run(mini_batch):
    azure_logger = AzureLogger(correlation_id=uuid4(), level=logging.DEBUG)

    results = pd.DataFrame(columns=["file", "predictions_ABSENT", "predictions_PRESENT"])

    for file_path in mini_batch:
        data = read_from_csv(
            data_file_path=file_path, azure_logger=azure_logger, index_column=None
        )

        predictions = model.predict_proba(data)

        azure_logger.log(
            f"Predictions for {file_path}",
            **{"classes": ["ABSENT", "PRESENT"]},
            **{"predictions": predictions},
        )

        predictions_dataframe = pd.DataFrame(
            predictions, columns=["predictions_ABSENT", "predictions_PRESENT"]
        )
        predictions_dataframe["file"] = os.path.basename(file_path)
        results = pd.concat([results, predictions_dataframe])

    return results
