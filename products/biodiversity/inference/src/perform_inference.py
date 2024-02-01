# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import argparse
import logging
import os
import sys
import time
import uuid
from ast import literal_eval

import pandas as pd
import utils.key_vault_utils as key_vault_utils
from argument_exception import ArgumentException
from azure.ai.ml import Input, MLClient
from azure.ai.ml.constants import AssetTypes
from azure.identity import ManagedIdentityCredential
from azure_logger import AzureLogger
from mlclient_utils import wait_until_job_finished


def perform_inference(
    site_name: str,
    species_list_csv: str,
    batch_input_path: str,
    merged_df_path: str,
    output_path: str,
    max_wait_time: int,
    subscription_id: str,
    resource_group: str,
    workspace: str,
    uid: str,
    azure_logger: AzureLogger = AzureLogger(
        correlation_id=uuid.uuid4(), level=logging.DEBUG
    ),
) -> None:
    """
    Iterates through all models, calls the batch endpoint for each of them,
    and saves the returned predictions.

    Parameters
    ----------
    site_name: str
        The site to perform inference for.
    species_list_csv: str
        Path to csv file containing the list of species to run inference for.
    model_version: str
        Name of model version which will be used to name the output file.
    batch_input_path: str
        Path from which to read the batch input.
    merged_df_path: str
        Path from which to read the merged dataframe to store inference results.
    output_path: str
        Path to output file for inference results.
    max_wait_time: int
        Maximum time in seconds to wait for a single job to complete.
    subscription_id: str
        Subscription id of the Azure ML workspace to connect to.
    resource_group: str
        Resource group of the Azure ML workspace to connect to.
    workspace: str
        Name of the Azure ML workspace to connect to.
    uid: str
        Uid of the deployed platform.
    azure_logger: AzureLogger
        Azure logger to use for logging.
    Returns
    -------
        None
    """

    # Load list of species from csv file
    species_df = pd.read_csv(species_list_csv)
    species = species_df["species"].tolist()
    species_names = [name.lower().replace(" ", "-") for name in species]

    # For testing, un-comment this line
    # species_names = ["burteo-jamaicensis", "melanitta-perspicillata"]

    azure_logger.log(f"{len(species_names)} species found in csv file.")
    azure_logger.log(f"{species_names}")

    # Retrieve common managed identity
    client_id = os.environ.get("DEFAULT_IDENTITY_CLIENT_ID")

    credentials = ManagedIdentityCredential(client_id=client_id)

    # create Azure ML client to invoke batch endpoint
    ml_client = MLClient(credentials, subscription_id, resource_group, workspace)

    # Invoke batch endpoints for alle species
    all_jobs = dict()
    for idx, species_name in enumerate(species_names):
        experiment_base_name = f"{site_name}-{species_name}"[:31]
        endpoint_name = f"{experiment_base_name}-{uid}"
        azure_logger.log(
            f"Invoking batch endpoint {endpoint_name} with input {batch_input_path}",
            **{
                "endpoint_name": endpoint_name,
                "species_name": species_name,
                "input": batch_input_path,
            },
        )
        input = Input(type=AssetTypes.URI_FOLDER, path=batch_input_path)
        job = ml_client.batch_endpoints.invoke(
            endpoint_name=endpoint_name,
            input=input,
        )
        all_jobs[species_name] = job.name
        azure_logger.log(f"Invoked batch endpoint {idx+1}/{len(species_names)}..")
        azure_logger.event(
            "Batch endpoint invoked",
            **{"endpoint_name": endpoint_name, "species_name": species_name},
        )

    merged_df = pd.read_csv(f"{merged_df_path}/merged_df.csv")
    proba_inf_df = merged_df.copy()

    # Download all predicitions and merge them into one data frame
    for idx, (species_name, job_name) in enumerate(all_jobs.items()):
        experiment_base_name = f"{site_name}-{species_name}"[:31]
        endpoint_name = f"{experiment_base_name}-{uid}"
        try:
            wait_until_job_finished(ml_client, job_name, max_wait_time, azure_logger)

            download_path = f"./{site_name}/{species_name}"

            if not os.path.exists(f"./{site_name}"):
                os.mkdir(f"./{site_name}")

            if not os.path.exists(download_path):
                os.mkdir(download_path)

            ml_client.jobs.download(
                name=job_name, output_name="score", download_path=download_path
            )

            # read downloaded file and add predictions to data frame with all results
            with open(f"{download_path}/predictions.csv", "r") as f:
                file_content = f.read().replace("\n", "'],['").replace(" ", "','")
                dataframe_list = f"[['{file_content}']]"
                predictions_df = pd.DataFrame(
                    literal_eval(dataframe_list),
                    columns=["file", "predictions_ABSENT", "predictions_PRESENT"],
                )
                proba_inf_df[f"model_output_{species_name}"] = predictions_df[
                    "predictions_PRESENT"
                ]

        except Exception as ex:
            azure_logger.event(
                "Exception downloading output",
                **{
                    "job_name": job_name,
                    "species_name": species_name,
                    "endpoint_name": endpoint_name,
                },
            )
            azure_logger.log(str(ex))
            raise
        azure_logger.log(f"Downloaded output {idx+1}/{len(species_names)}..")
        azure_logger.event(
            "Downloaded output",
            **{
                "species_name": species_name,
                "endpoint_name": endpoint_name,
                "job_name": job_name,
            },
        )

    proba_inf_df.drop(proba_inf_df.columns[0], axis=1, inplace=True)
    proba_inf_df.to_csv(output_path)


def main(argv):
    args = __define_arguments(argv)

    if args.key_vault_name is not None:
        key_vault_utils.get_environment_from_kv(args.key_vault_name)

    azure_logger = AzureLogger(correlation_id=args.correlation_id, level=logging.DEBUG)

    azure_logger.event("starting perform_inference")

    start_time = time.time()

    try:
        perform_inference(
            site_name=args.site_name,
            species_list_csv=args.species_list_csv,
            batch_input_path=args.batch_input_path,
            merged_df_path=args.merged_df_path,
            output_path=args.output_path,
            max_wait_time=args.max_wait_time,
            subscription_id=args.subscription_id,
            resource_group=args.resource_group,
            workspace=args.workspace,
            uid=args.uid,
            azure_logger=azure_logger,
        )
    except ArgumentException:
        sys.exit(2)
    except Exception as ex:
        azure_logger.exception(ex, "perform_inference.py failed")
        sys.exit(1)
    finally:
        end_time = time.time()

        azure_logger.event(
            "completed perform_inference",
            **{"duration (s)": str((end_time - start_time))},
        )


def __define_arguments(args):
    parser = argparse.ArgumentParser("perform_inference")

    parser.add_argument(
        "--site_name",
        type=str,
        required=True,
        help="The site for which to perform inference.",
    )

    parser.add_argument(
        "--species_list_csv",
        type=str,
        required=True,
        help="CSV file with list of species to perform inference for.",
    )

    parser.add_argument(
        "--batch_input_path",
        type=str,
        required=True,
        help="Path to the batch input.",
    )

    parser.add_argument(
        "--merged_df_path",
        type=str,
        required=True,
        help="Path to the merged dataframe to use to store inference results.",
    )

    parser.add_argument(
        "--output_path",
        type=str,
        required=True,
        help="File where the predictions output will be stored.",
    )

    parser.add_argument(
        "--max_wait_time",
        type=int,
        required=True,
        help="Maximum time in seconds to wait for a single job to complete.",
    )

    parser.add_argument(
        "--subscription_id",
        type=str,
        required=True,
        help="Subscription id of the Azure ML workspace",
    )

    parser.add_argument(
        "--resource_group",
        type=str,
        required=True,
        help="Resource group of the Azure ML workspace.",
    )

    parser.add_argument(
        "--workspace",
        type=str,
        required=True,
        help="Name of the Azure ML workspace",
    )

    parser.add_argument(
        "--correlation_id",
        type=str,
        required=False,
        default=uuid.uuid4(),
        help="Application Insights correlation id if required.",
    )

    parser.add_argument(
        "--key_vault_name",
        type=str,
        required=False,
        help="Key Vault name where to retrieve secrets.",
    )

    parser.add_argument(
        "--uid", type=str, required=True, help="Uid of the deployed platform."
    )

    return parser.parse_args(args)


if __name__ == "__main__":
    main(sys.argv[1:])
