# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import argparse
import json
import logging
import os
import sys
import time
import uuid

import requests
from argument_exception import ArgumentException
from azure_logger import AzureLogger
from requests.structures import CaseInsensitiveDict


def azdo_pipeline_callback(
    model_metadata_string: str,
    score_path_string: str,
    buildid: str,
    pipeline_pat: str,
    azdo_pipeline_rest_version: str,
    project_name: str,
    org_name: str,
    register_pipeline_version_number: str,
    register_pipeline_definition_number: str,
    branch_name: str,
    experiment_base_name: str,
    correlation_id: uuid.UUID,
) -> None:
    """
    Completes the MLOps pipeline with a callback to the Azure Dev Ops
    endpoint.

    Parameters
    ----------
    model_metadata_string: str
        Path to input model metadata.
    score_path_string: str
        Path to folder with scores.
    buildid: str
        ADO Build Id,
    pipeline_pat: str
        PAT for Azure DevOps Rest API authentication.
    azdo_pipeline_rest_version: str
        Azure DevOps Rest API version to use for callback.
    project_name: str
        The name of the Azure DevOps project used for callback.
    org_name: str
        The name of the Azure DevOps Organization used for callback.
    register_pipeline_version_number: str
        Azure DevOps pipeline version to use for callback
    register_pipeline_definition_number: str
        Azure DevOps pipeline definition number to use for callback.
    branch_name: str
        Azure DevOps target branch name.
    experiment_base_name: str
        Base name for the experiment consisting of site and species name.
    correlation_id: uuid
        Optional Application Insights correlation id.
    """

    azure_logger = AzureLogger(correlation_id=correlation_id, level=logging.DEBUG)

    with open(model_metadata_string) as run_file:
        model_metadata = json.load(run_file)

    run_id = model_metadata["run_id"]
    run_uri = model_metadata["run_uri"]

    score_path = os.path.join(score_path_string, "metrics.json")

    with open(score_path) as score_file:
        score_data = json.load(score_file)

    roc_auc = score_data["roc_auc"]
    accuracy = score_data["clf_report"]["accuracy"]

    headers = CaseInsensitiveDict()
    basic_auth_credentials = ("", pipeline_pat)
    headers["Content-Type"] = "application/json"

    request_body = {
        "resources": {"repositories": {"self": {"refName": branch_name}}},
        "templateParameters": {
            "runid": run_id,
            "runuri": run_uri,
            "accuracy": accuracy,
            "roc_auc": roc_auc,
            "experiment_base_name": experiment_base_name,
            "trigger_buildid": buildid,
        },
    }

    url = (
        "https://dev.azure.com/{}/{}/_apis/pipelines/{}/"
        + "runs?pipelineVersion={}&api-version={}"
    ).format(
        org_name,
        project_name,
        register_pipeline_definition_number,
        register_pipeline_version_number,
        azdo_pipeline_rest_version,
    )

    azure_logger.event(
        "pipeline callback", **{"url": url, "request_body": json.dumps(request_body)}
    )

    resp = requests.post(
        url, auth=basic_auth_credentials, headers=headers, json=request_body
    )

    azure_logger.log("response code", **{"response_code": resp.status_code})

    if int(resp.status_code) != 200:
        raise Exception(
            f"Pipeline was unsuccessful. The return code is: {resp.status_code}. "
            + f"Reason is {resp.reason}"
        )


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

    azure_logger.event("starting azdo_pipeline_callback")
    start_time = time.time()
    try:
        azdo_pipeline_callback(
            model_metadata_string=args.model_metadata,
            score_path_string=args.score_path,
            buildid=args.buildid,
            pipeline_pat=args.pipeline_pat,
            azdo_pipeline_rest_version=args.azdo_pipeline_rest_version,
            project_name=args.project_name,
            org_name=args.org_name,
            register_pipeline_version_number=args.register_pipeline_version_number,
            register_pipeline_definition_number=args.register_pipeline_definition_number,
            branch_name=args.branch_name,
            experiment_base_name=args.experiment_base_name,
            correlation_id=args.correlation_id,
        )

    except ArgumentException:
        sys.exit(2)
    except Exception as ex:
        azure_logger.exception(ex, "azdo_pipeline_callback failed")
        sys.exit(1)
    finally:
        end_time = time.time()
        azure_logger.event(
            "completed azdo_pipeline_callback",
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
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--model_metadata", required=True, type=str, help="Path to input model metadata"
    )
    parser.add_argument(
        "--score_path", required=True, type=str, help="Path to folder with scores"
    )
    parser.add_argument("--buildid", required=True, type=str, help="ADO Build Id")
    parser.add_argument(
        "--pipeline_pat",
        required=True,
        type=str,
        help="PAT for Azure DevOps Rest API authentication",
    )
    parser.add_argument(
        "--azdo_pipeline_rest_version",
        required=True,
        type=str,
        help="Azure DevOps Rest API version to use for callback",
    )
    parser.add_argument(
        "--project_name",
        type=str,
        required=True,
        help="The name of the Azure DevOps project used for callback",
    )
    parser.add_argument(
        "--org_name",
        type=str,
        required=True,
        help="The name of the Azure DevOps Organization used for callback",
    )
    parser.add_argument(
        "--register_pipeline_version_number",
        required=True,
        type=str,
        help="Azure DevOps pipeline version to use for callback",
    )
    parser.add_argument(
        "--register_pipeline_definition_number",
        required=True,
        type=str,
        help="Azure DevOps pipeline definition number to use for callback",
    )
    parser.add_argument(
        "--deploy_environment",
        type=str,
        required=True,
        help="Azure DevOps pipeline target deployment environment e.g. dev test prod",
    )
    parser.add_argument(
        "--branch_name", type=str, required=True, help="Azure DevOps target branch name"
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
