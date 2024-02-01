# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import argparse
import logging
import os
import sys
import time
import uuid

from argument_exception import ArgumentException
from azure.ai.ml import MLClient
from azure.ai.ml.constants import BatchDeploymentOutputAction
from azure.ai.ml.entities import (
    BatchDeployment,
    BatchEndpoint,
    BatchRetrySettings,
    CodeConfiguration,
    Environment,
)
from azure.identity import DefaultAzureCredential
from azure_logger import AzureLogger


def batch_deployment(
    subscription_id: str,
    uid: str,
    resource_group_name: str,
    workspace_name: str,
    cluster_name: str,
    env_base_image_name: str,
    conda_path: str,
    experiment_base_name: str,
):
    """
    Batch deployment

    Parameters
    ----------
    subscription_id: str,
        Azure subscription id.
    uid: str,
        Uid of the deployed platform.
    resource_group_name: str,
        Azure Machine learning resource group.
    workspace_name: str,
        Azure Machine learning Workspace name.
    cluster_name: str,
        Azure Machine learning cluster name.
    env_base_image_name: str,
        Environment base image.
    conda_path: str,
        Path to the conda environment configuration file.
    experiment_base_name: str,
        Base name for the experiment consisting of site and species name.

    Returns
    -------
        None
    """

    ml_client = MLClient(
        DefaultAzureCredential(),
        subscription_id,
        resource_group_name,
        workspace_name,
    )

    model_name = f"{experiment_base_name}-model"

    # Select latest version of model
    model_versions = ml_client.models.list(name=model_name)

    latest_model_version = max([int(model.version) for model in model_versions])

    model = ml_client.models.get(name=model_name, version=latest_model_version)

    # Capping the name of the endpoint to 32 characters and concatenate uid
    endpoint_and_batch_name = f"{experiment_base_name[:31]}-{uid}"

    # Define and create endpoint
    endpoint = BatchEndpoint(
        name=endpoint_and_batch_name,
        description=f"Batch endpoint for {experiment_base_name}",
    )

    ml_client.batch_endpoints.begin_create_or_update(endpoint).wait(360)

    # Select conda environment
    environment = Environment(
        conda_file=os.path.join(os.getcwd(), conda_path),
        image=env_base_image_name,
    )

    # Select code configuration
    code_configuration = CodeConfiguration(
        code=os.path.join(os.getcwd(), "products/"),
        scoring_script="biodiversity/src/mlops/scoring_src/scoring.py",
    )

    # Define batch deployment
    deployment = BatchDeployment(
        name=endpoint_and_batch_name,
        description=f"Deployment for {experiment_base_name}",
        endpoint_name=endpoint.name,
        model=model,
        environment=environment,
        environment_variables={"PYTHONPATH": "./common/src:"},
        code_configuration=code_configuration,
        compute=cluster_name,
        instance_count=2,
        max_concurrency_per_instance=2,
        mini_batch_size=2,
        output_action=BatchDeploymentOutputAction.APPEND_ROW,
        output_file_name="predictions.csv",
        retry_settings=BatchRetrySettings(max_retries=3, timeout=300),
        logging_level="info",
    )

    # Initiate deployment
    ml_client.batch_deployments.begin_create_or_update(deployment).wait(360)

    # Set default deployment
    endpoint = ml_client.batch_endpoints.get(endpoint_and_batch_name)
    endpoint.defaults.deployment_name = deployment.name
    ml_client.batch_endpoints.begin_create_or_update(endpoint)


def main(argv) -> None:
    """
    Main - executes the script.

    Parameters
    ----------
    argv: list[str]
        Command line arguments presented as a list of strings.
    """
    args = __define_arguments(argv)

    azure_logger = AzureLogger(correlation_id=args.correlation_id, level=logging.DEBUG)

    azure_logger.event("starting batch_deployment")
    start_time = time.time()
    try:
        batch_deployment(
            cluster_name=args.cluster_name,
            conda_path=args.conda_path,
            env_base_image_name=args.env_base_image_name,
            experiment_base_name=args.experiment_base_name,
            resource_group_name=args.resource_group_name,
            subscription_id=args.subscription_id,
            workspace_name=args.workspace_name,
            uid=args.uid,
        )

    except ArgumentException:
        sys.exit(2)
    except Exception as ex:
        azure_logger.exception(ex, "batch_deployment failed")
        sys.exit(1)
    finally:
        end_time = time.time()
        azure_logger.event(
            "completed batch_deployment",
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
    parser = argparse.ArgumentParser("batch_deployment")

    parser.add_argument(
        "--subscription_id", type=str, required=True, help="Azure subscription id"
    )
    parser.add_argument(
        "--uid", type=str, required=True, help="Uid of the deployed platform"
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
        "--cluster_name",
        type=str,
        required=True,
        help="Azure Machine learning cluster name",
    )
    parser.add_argument(
        "--env_base_image_name",
        type=str,
        required=True,
        help="Environment base image",
    )
    parser.add_argument(
        "--conda_path",
        type=str,
        required=True,
        help="Path to the conda environment configuration file",
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

    return parser.parse_args(args)


if __name__ == "__main__":
    main(sys.argv[1:])
