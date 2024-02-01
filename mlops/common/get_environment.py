# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

from azure.ai.ml import MLClient
from azure.identity import DefaultAzureCredential
import argparse
from azure.ai.ml.entities import Environment
import os


def get_environment(
    subscription_id: str,
    resource_group_name: str,
    workspace_name: str,
    env_base_image_name: str,
    conda_path: str,
    environment_name: str,
    environment_version: str,
    description: str,
):
    try:
        print(f"Checking {environment_name} environment.")  # noqa:T201
        client = MLClient(
            DefaultAzureCredential(),
            subscription_id=subscription_id,
            resource_group_name=resource_group_name,
            workspace_name=workspace_name,
        )
        env_docker_conda = Environment(
            image=env_base_image_name,
            conda_file=conda_path,
            name=environment_name,
            version=environment_version,
            description=description,
        )
        environment = client.environments.create_or_update(env_docker_conda)
        print(f"Environment {environment_name} has been created or updated.")  # noqa:T201
        print(f"this is from python = = {environment.version}")  # noqa:T201
        os.environ["environment_version"] = environment.version
        return environment.version

    except Exception as ex:
        print(  # noqa:T201
            "Oops! invalid credentials or error while creating ML environment. Try again."
        )
        raise ex


def main():
    parser = argparse.ArgumentParser("prepare_environment")
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
        "--env_base_image_name",
        type=str,
        required=True,
        help="Environment custom base image name",
    )
    parser.add_argument(
        "--conda_path", type=str, required=True, help="path to conda requirements file"
    )
    parser.add_argument(
        "--environment_name",
        type=str,
        required=True,
        help="Azure Machine learning environment name",
    )
    parser.add_argument(
        "--environment_version",
        type=str,
        required=True,
        help="Azure Machine learning environment version",
    )
    parser.add_argument(
        "--description", type=str, required=True, default="Environment created using Conda."
    )

    args = parser.parse_args()

    get_environment(
        args.subscription_id,
        args.resource_group_name,
        args.workspace_name,
        args.env_base_image_name,
        args.conda_path,
        args.environment_name,
        args.environment_version,
        args.description,
    )


if __name__ == "__main__":
    main()
