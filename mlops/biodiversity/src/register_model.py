# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import argparse
import logging
import sys
import time
import uuid

import mlflow
from argument_exception import ArgumentException
from azure.ai.ml import MLClient
from azure.ai.ml.constants import AssetTypes
from azure.ai.ml.entities import Model
from azure.identity import DefaultAzureCredential
from azure_logger import AzureLogger


def register_model(
    subscription_id: str,
    resource_group_name: str,
    workspace_name: str,
    experiment_base_name: str,
    model_run_path: str,
    accuracy: str,
    roc_auc: str,
    trigger_buildid: str,
    correlation_id: uuid.UUID,
) -> None:
    """
    Registers the model.

    Parameters:
    subscription_id: str
        Azure subscription id.
    resource_group_name: str
        Azure Machine learning resource group.
    workspace_name: str
        Azure Machine learning Workspace nam.
    model_run_path: str
        model path on Machine Learning Workspace.
    accuracy: str
        Accuracy Metric of the new model based on training.
    roc_auc: str
        ROC AUC score of the new model based on training.
    trigger_buildid: str
        Original AzDo build id that initiated experiment.
    experiment_base_name: str
        Base name for the experiment consisting of site and species name
    correlation_id: str
        Application Insights correlation id if required.
    Returns:
    None
    """
    azure_logger = AzureLogger(correlation_id=correlation_id, level=logging.DEBUG)

    client = MLClient(
        DefaultAzureCredential(),
        subscription_id=subscription_id,
        resource_group_name=resource_group_name,
        workspace_name=workspace_name,
    )

    azureml_mlflow_uri = client.workspaces.get(workspace_name).mlflow_tracking_uri
    mlflow.set_tracking_uri(azureml_mlflow_uri)
    try:
        credential = DefaultAzureCredential()
        credential.get_token("https://management.azure.com/.default")
    except Exception as ex:
        azure_logger.exception(ex, "Invalid credentials.")
        raise

    model_name = f"{experiment_base_name}-model"

    try:
        run_model = Model(
            path=model_run_path,
            name=model_name,
            description="Model creation from experiment.",
            type=AssetTypes.MLFLOW_MODEL,
            properties={
                "accuracy": accuracy,
                "roc_auc": roc_auc,
                "trigger_buildid": trigger_buildid,
            },
        )

        client.models.create_or_update(run_model)
    except Exception as ex:
        azure_logger.exception(ex, "Model registration was unsuccessful.")
        sys.exit(1)


def main(argv) -> None:
    args = __define_arguments(argv)

    azure_logger = AzureLogger(correlation_id=args.correlation_id, level=logging.DEBUG)

    azure_logger.event(
        "starting register_model",
    )
    start_time = time.time()
    try:
        register_model(
            subscription_id=args.subscription_id,
            accuracy=args.accuracy,
            experiment_base_name=args.experiment_base_name,
            model_run_path=args.model_run_path,
            resource_group_name=args.resource_group_name,
            roc_auc=args.roc_auc,
            workspace_name=args.workspace_name,
            trigger_buildid=args.trigger_buildid,
            correlation_id=args.correlation_id,
        )
    except ArgumentException:
        sys.exit(2)
    except Exception as ex:
        azure_logger.exception(
            ex,
            "register_model failed",
        )
        sys.exit(1)
    finally:
        end_time = time.time()
        azure_logger.event(
            "completed register_model",
            **{
                "duration (s)": str((end_time - start_time)),
            },
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
    parser = argparse.ArgumentParser("register_model")

    parser.add_argument(
        "--subscription_id", type=str, required=True, help="Azure subscription id"
    )
    parser.add_argument(
        "--resource_group_name",
        type=str,
        required=True,
        help="Azure Machine learning resource group",
    )
    parser.add_argument(
        "--workspace_name",
        type=str,
        required=True,
        help="Azure Machine learning Workspace name",
    )
    parser.add_argument(
        "--model_run_path",
        type=str,
        required=True,
        help="model path on Machine Learning Workspace",
    )
    parser.add_argument(
        "--accuracy",
        required=True,
        type=str,
        help="Accuracy Metric of the new model based on training",
    )
    parser.add_argument(
        "--roc_auc",
        type=str,
        required=True,
        help="ROC AUC score of the new model based on training",
    )
    parser.add_argument(
        "--trigger_buildid",
        type=str,
        required=True,
        help="Original AzDo build id that initiated experiment",
    )

    parser.add_argument(
        "--experiment_base_name",
        type=str,
        required=True,
        help="Base name for the experiment consisting of site and species name",
    )

    parser.add_argument(
        "--correlation_id",
        type=uuid.UUID,
        required=False,
        default=uuid.uuid4(),
        help="Application Insights correlation id if required.",
    )

    return parser.parse_args()


if __name__ == "__main__":
    main(sys.argv[1:])
