# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""scoring logic."""

import argparse
import logging
import os
import sys
import time
from typing import List, Optional, Tuple
from uuid import uuid4

import matplotlib.pyplot as plt
import mlflow
import mlflow.lightgbm
import pandas as pd
import seaborn as sns
from azure_logger import AzureLogger
from lightgbm import LGBMClassifier
from PIL import Image
from sklearn.metrics import classification_report, confusion_matrix, roc_auc_score
from utils.read_utils import read_from_config_file, read_from_csv
from utils.write_utils import write_to_json


def main(
    scoring_data_path,
    best_parameters_path,
    scores_path,
    correlation_id,
):
    if correlation_id is None:
        correlation_id = uuid4()

    azure_logger = AzureLogger(correlation_id=correlation_id, level=logging.DEBUG)

    start_time = time.time()

    try:
        features_training_path = os.path.join(
            scoring_data_path, "training_features_data.csv"
        )
        features_training_dataframe = read_from_csv(
            features_training_path, azure_logger, index_column=0
        )

        targets_training_path = os.path.join(scoring_data_path, "training_targets_data.csv")
        targets_training_dataframe = read_from_csv(
            targets_training_path, azure_logger, index_column=0
        )

        features_testing_path = os.path.join(scoring_data_path, "testing_features_data.csv")
        features_testing_dataframe = read_from_csv(
            features_testing_path, azure_logger, index_column=0
        )

        targets_testing_path = os.path.join(scoring_data_path, "testing_targets_data.csv")
        targets_testing_dataframe = read_from_csv(
            targets_testing_path, azure_logger, index_column=0
        )

        best_parameter_space_path = os.path.join(
            best_parameters_path, "best_parameters.yml"
        )
        best_parameter_space = read_from_config_file(
            best_parameter_space_path, azure_logger
        )

        model = LGBMClassifier(**best_parameter_space)

        model.fit(features_training_dataframe, targets_training_dataframe, verbose=0)

        targets_predicted = model.predict(features_testing_dataframe)

        metrics, confusion_m_df, clf_report = evaluate(
            targets_data=targets_testing_dataframe.values.tolist(),
            targets_predicted=targets_predicted,
            classes=["ABSENT", "PRESENT"],
        )

        write_results(metrics, confusion_m_df, clf_report, scores_path, azure_logger)

    except BaseException as ex:
        azure_logger.exception(ex, __name__)
    finally:
        end_time = time.time()

        azure_logger.event(
            "completed score.py",
            **{"duration (s)": str((end_time - start_time))},
        )


def evaluate(
    targets_data: List[float],
    targets_predicted: List[float],
    classes: Optional[List[str]] = None,
) -> Tuple[dict, pd.DataFrame, dict]:
    """
    Evaluates model and returns a dictionary with the following reports:
    * Classification report (sklearn classification report)
    * Confusion Matrix
    * AUC-ROC

    Parameters
    ----------
    targets_data : List[float]
        Actual values vector
    targets_predicted : List[float]
        Predicted values vector
    classes : List[str], optional
        Classes, by default None

    Returns
    -------
    metrics : dict
        A dictionary, containing reports
    confusion_m_df : pd.DataFrame
        A dataframe containing the confusion matrix
    classification_report : dict
        A dictionary containing the classification report

    """
    metrics = {}

    # Calculate ROC AUC score
    roc_auc = roc_auc_score(targets_data, targets_predicted)
    metrics["roc_auc"] = roc_auc

    mlflow.log_metric("roc_auc", roc_auc)

    clf_report_labels = {
        "ABSENT class label": 0,
        "PRESENT class label": 1,
        "Macro Average class label": 2,
        "Weighted Average class label": 3,
    }
    mlflow.log_metrics(clf_report_labels)

    # Calculate classification report
    clf_report = classification_report(targets_data, targets_predicted, output_dict=True)
    metrics["clf_report"] = clf_report

    mlflow.log_metric("accuracy", clf_report["accuracy"])
    mlflow.log_metrics(clf_report["0"])
    mlflow.log_metrics(clf_report["1"])
    mlflow.log_metrics(clf_report["macro avg"])
    mlflow.log_metrics(clf_report["weighted avg"])

    # Calculate confusion matrix
    confusion_m = confusion_matrix(targets_data, targets_predicted)
    confusion_m_df = pd.DataFrame(confusion_m, index=classes, columns=classes)

    metrics["confusion_m"] = confusion_m_df.to_dict()

    return metrics, confusion_m_df, clf_report


def write_results(
    metrics: dict,
    confusion_m_df: pd.DataFrame,
    classification_report: dict,
    output_folder: str,
    azure_logger: AzureLogger,
) -> None:
    """
    Write the results of the evaluation to a folder.

    Parameters
    ----------
    metrics : dict
        A dictionary, containing reports
    confusion_m_df : pd.DataFrame
        A dataframe containing the confusion matrix
    classification_report : dict
        A dictionary containing the classification report
    output_folder : str
        Folder where all the results will be written
    azure_logger: AzureLogger
        AzureLogger instance that communicates with AppInsights.

    Returns
    -------
    None

    """

    confusion_m_plot = sns.heatmap(confusion_m_df, annot=True, fmt="g")
    confusion_m_plot.figure.savefig(os.path.join(output_folder, "cfm.png"))

    mlflow.log_image(
        Image.open(os.path.join(output_folder, "cfm.png")), "confusion_matrix.png"
    )

    plt.clf()

    clf_plot = sns.heatmap(
        pd.DataFrame(classification_report).iloc[:-1, :].T, annot=True, fmt=".3f"
    )
    clf_plot.figure.savefig(os.path.join(output_folder, "clf_report.png"))

    mlflow.log_image(
        Image.open(os.path.join(output_folder, "clf_report.png")), "clf_report.png"
    )

    # Save evaluation results
    write_to_json(os.path.join(output_folder, "metrics.json"), metrics, azure_logger)


def __define_arguments(args):
    parser = argparse.ArgumentParser("score")

    parser.add_argument(
        "--scoring_data_path",
        type=str,
        required=True,
        help="Path to the folder containing the features and targets for scoring",
    )

    parser.add_argument(
        "--best_parameters_path",
        type=str,
        required=True,
        help="Path to the folder where the optimal parameters are kept",
    )

    parser.add_argument(
        "--scores_path",
        type=str,
        required=False,
        help="Path to the folder where the score data will be stored",
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
        args.scoring_data_path,
        args.best_parameters_path,
        args.scores_path,
        args.correlation_id,
    )
