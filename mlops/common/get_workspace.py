# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

from azure.ai.ml import MLClient
from azure.identity import DefaultAzureCredential
import argparse


def get_workspace(subscription_id: str, resource_group_name: str, workspace_name: str):
    try:
        print(f"Getting access to {workspace_name} workspace.")  # noqa:T201
        client = MLClient(
            DefaultAzureCredential(),
            subscription_id=subscription_id,
            resource_group_name=resource_group_name,
            workspace_name=workspace_name,
        )

        workspace = client.workspaces.get(workspace_name)
        print(f"Reference to {workspace_name} has been obtained.")  # noqa:T201
        return workspace
    except Exception as ex:
        print("Oops!  invalid credentials.. Try again...")  # noqa:T201
        raise ex


def main():
    parser = argparse.ArgumentParser("get_workspace")
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

    args = parser.parse_args()
    get_workspace(args.subscription_id, args.resource_group_name, args.workspace_name)


if __name__ == "__main__":
    main()
