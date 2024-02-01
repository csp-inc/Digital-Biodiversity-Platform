# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import argparse
import logging
import os
import sys
import time
import uuid

import mlflow
from argument_exception import ArgumentException
from azure.ai.ml import Input, MLClient, Output, command, load_component
from azure.ai.ml.dsl import pipeline
from azure.identity import DefaultAzureCredential
from azure_logger import AzureLogger
from utils.read_utils import lower_replace


def mlops_pipeline(
    subscription_id: str,
    resource_group_name: str,
    workspace_name: str,
    cluster_name: str,
    deploy_environment: str,
    environment_name: str,
    build_reference: str,
    wait_for_completion: str,
    pipeline_pat: str,
    azdo_pipeline_rest_version: str,
    project_name: str,
    org_name: str,
    register_pipeline_version_number: str,
    register_pipeline_definition_number: str,
    branch_name: str,
    site_path: str,
    site_name: str,
    species_name: str,
    number_of_folds: int,
    maximum_evaluations: int,
    correlation_id: uuid.UUID,
) -> None:
    """
    Runs the MLOps pipeline

    Parameters:
    -----------
    subscription_id: str
        Azure subscription id.
    resource_group_name: str
        Azure Machine learning resource group.
    workspace_name: str
        Azure Machine learning Workspace name.
    cluster_name: str
        Azure Machine learning cluster name.
    deploy_environment: str
        Execution and deployment environment. e.g. dev, prod, test.
    environment_name: str
        Azure Machine Learning Environment name for job execution.
    build_reference: str
        Unique identifier for Azure DevOps pipeline run.
    wait_for_completion: str
        Determine if pipeline to wait for job completion.
    pipeline_pat: str
        PAT for calling Azure DevOps Rest API authentication.
    azdo_pipeline_rest_version: str
        Azure DevOps Rest API version to use for callback.
    project_name: str
        The name of the Azure DevOps project used for callback.
    org_name: str
        The name of the Azure DevOps Organization used for callback.
    register_pipeline_version_number: str
        Azure DevOps pipeline version to use for callback.
    register_pipeline_definition_number: str
        Azure DevOps pipeline definition number to use for callback.
    branch_name: str
        Azure DevOps target branch name.
    site_path: str
        Storage path to the site specific folder to read data from.
    site_name: str
        Name of the site being processed.
    species_name: str
        Name of the species being processed.
    number_of_folds: int
        Required number of folds.
    maximum_evaluations: int
        Maximum number of evaluations.
    correlation_id: uuid.UUID
        Application Insights correlation id.

    Returns:
    --------
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

        client.compute.get(cluster_name)

    except Exception as ex:
        azure_logger.exception(ex, "Unable to connect to compute.")
        raise

    # Manually defined inputs for the pipeline
    parent_dir = os.path.join(os.getcwd(), "products/biodiversity/src/mlops")

    # azureml:<site-name>-training-features:@latest
    raw_data_gbif_asset = f"{site_name}-training-features"
    raw_data_gbif_version = next(
        i.latest_version for i in client.data.list() if i.name == raw_data_gbif_asset
    )
    raw_data_gbif_path = f"azureml:{raw_data_gbif_asset}:{raw_data_gbif_version}"

    species_file_name = lower_replace(species_name, "-", "_") + "_golddata.csv"

    # azureml:<site-name>-training-species:@latest
    raw_data_species_asset = f"{site_name}-training-species"
    raw_data_species_version = next(
        i.latest_version for i in client.data.list() if i.name == raw_data_species_asset
    )
    raw_data_species_path = f"azureml:{raw_data_species_asset}:{raw_data_species_version}"

    parameters_path = os.path.join(os.getcwd(), "mlops/biodiversity/config/lgbm_params.yml")

    columns_to_drop = (
        "soil_mukey_total gbifid decimallatitude decimallongitude species year month"
    )

    drop_nans = True
    target_col_name = "occurrencestatus"
    train_fraction = 0.9
    random_seed = 42
    hyperparameter_algorithm = "tpe"
    verbose = -1

    # Make species and site name compatible with Azure naming convention
    species_name = lower_replace(species_name, " ", "-")
    site_name = lower_replace(site_name, " ", "-")

    experiment_base_name = f"{site_name}-{species_name}"
    ml_pipeline_name = (
        f"{experiment_base_name}-lgbm-classification-{deploy_environment}-{build_reference}"
    )
    experiment_name = f"{site_name}-{deploy_environment}-{build_reference}"

    # Load the components of the ML Pipeline
    prepare_data = load_component(source=parent_dir + "/prep.yml")
    train_model = load_component(source=parent_dir + "/train.yml")
    score_data = load_component(source=parent_dir + "/score.yml")
    train_to_deploy = load_component(source=parent_dir + "/train_to_deploy.yml")

    # Set the environment name to custom environment using name and version number
    prepare_data.environment = f"azureml:{environment_name}:{build_reference}"
    train_model.environment = f"azureml:{environment_name}:{build_reference}"
    score_data.environment = f"azureml:{environment_name}:{build_reference}"
    train_to_deploy.environment = f"azureml:{environment_name}:{build_reference}"

    ai_connection_string = os.environ["APPLICATIONINSIGHTS_CONNECTION_STRING"]

    if wait_for_completion == "False":
        callback_pipeline = command(
            name="callback-azdo-pipeline",
            display_name="Callback Azure DevOps pipeline to register model",
            description="Callback Azure DevOps pipeline to register model",
            command="python mlops/biodiversity/src/azdo_pipeline_callback.py --model_metadata ${{inputs.model_metadata}} --score_path ${{inputs.score_path}} --buildid ${{inputs.buildid}} --pipeline_pat ${{inputs.pipeline_pat}} --azdo_pipeline_rest_version ${{inputs.azdo_pipeline_rest_version}} --project_name ${{inputs.project_name}} --org_name ${{inputs.org_name}} --deploy_environment ${{inputs.deploy_environment}} --branch_name ${{inputs.branch_name}} --register_pipeline_version_number ${{inputs.register_pipeline_version_number}} --register_pipeline_definition_number ${{inputs.register_pipeline_definition_number}} --experiment_base_name ${{inputs.experiment_base_name}} --correlation_id ${{inputs.correlation_id}}",  # noqa: E501
            environment=f"azureml:{environment_name}:{build_reference}",
            environment_variables={
                "PYTHONPATH": "./products/common/src:",
                "APPLICATIONINSIGHTS_CONNECTION_STRING": ai_connection_string,
            },
            inputs=dict(
                model_metadata=Input(type="uri_file"),
                score_path=Input(type="uri_folder"),
                buildid=build_reference,
                pipeline_pat=pipeline_pat,
                azdo_pipeline_rest_version=azdo_pipeline_rest_version,
                project_name=project_name,
                org_name=org_name,
                register_pipeline_version_number=register_pipeline_version_number,
                register_pipeline_definition_number=register_pipeline_definition_number,
                deploy_environment=deploy_environment,
                branch_name=branch_name,
                experiment_base_name=experiment_base_name,
                correlation_id=str(correlation_id),
            ),
            outputs=dict(output_folder=Output(type="uri_folder", mode="rw_mount")),
            code=".",
        )

    @pipeline(
        name=ml_pipeline_name,
        display_name=ml_pipeline_name,
        experiment_name=experiment_name,
        tags={"environment": deploy_environment, "build_reference": build_reference},
    )
    def mlops_species(
        raw_data_gbif_path,
        raw_data_species_path,
        species_file_name,
        columns_to_drop,
        drop_nans,
        target_col_name,
        correlation_id,
        parameters_path,
        train_fraction,
        random_seed,
        number_of_folds,
        maximum_evaluations,
        hyperparameter_algorithm,
        verbose,
    ):
        """
        Define the pipeline and the inputs required throughout the stages.
        Outputs from certain stages can be sequenced and used as inputs for
        other stages.

        Parameters:
        -----------
        raw_data_gbif_path: Input,
            Path to the raw gbif data.
        raw_data_species_path: Input,
            Path to raw species data.
        species_file_name : str
            Name of the species file.
        columns_to_drop: str,
            CSV list of columns to drop.
        drop_nans: bool,
            Whether to drop NANs.
        target_col_name: str,
            Name of the target column.
        correlation_id: str,
            Telemetry correlation id.
        parameters_path: Input,
            Path to the parameters.
        train_fraction: float,
            Fraction of data to use for training.
        random_seed: int,
            Random seed.
        number_of_folds: int,
            Number of folds.
        maximum_evaluations: int,
            Maximum number of iterations.
        hyperparameter_algorithm: str,
            Name of the hyperparmater algorithm to use.
        verbose: int,
            Whether to use Verbose logging.

        Returns:
        --------
        None
        """
        prepare_sample_data = prepare_data(
            raw_data_gbif_path=raw_data_gbif_path,
            raw_data_species_path=raw_data_species_path,
            species_filename=species_file_name,
            columns_to_drop=columns_to_drop,
            drop_nans=drop_nans,
            target_col_name=target_col_name,
            correlation_id=correlation_id,
        )
        train_with_sample_data = train_model(
            input_path=prepare_sample_data.outputs.prep_data_path,
            parameters_path=parameters_path,
            train_fraction=train_fraction,
            random_seed=random_seed,
            number_of_folds=number_of_folds,
            maximum_evaluations=maximum_evaluations,
            hyperparameter_algorithm=hyperparameter_algorithm,
            verbose=verbose,
            correlation_id=correlation_id,
        )

        score_with_sample_data = score_data(
            scoring_data_path=train_with_sample_data.outputs.scoring_data_path,
            best_parameters_path=train_with_sample_data.outputs.best_parameters_path,
            correlation_id=correlation_id,
        )

        training_to_deploy = train_to_deploy(
            prep_data_path=prepare_sample_data.outputs.prep_data_path,
            best_parameters_path=train_with_sample_data.outputs.best_parameters_path,
            correlation_id=correlation_id,
        )

        if wait_for_completion == "False":
            callback_pipeline(
                model_metadata=training_to_deploy.outputs.model_metadata,
                score_path=score_with_sample_data.outputs.scores_path,
                correlation_id=correlation_id,
            )
        return {
            "pipeline_job_prepped_data": prepare_sample_data.outputs.prep_data_path,
            "pipeline_job_trained_model_params": train_with_sample_data.outputs.best_parameters_path,  # noqa: E501
            "pipeline_job_trained_model_data": train_with_sample_data.outputs.scoring_data_path,  # noqa: E501
            "pipeline_job_score_report": score_with_sample_data.outputs.scores_path,
            "pipeline_job_final_model": training_to_deploy.outputs.model_output_path,
        }

    # Call and run pipeline with the desired arguments
    pipeline_job = mlops_species(
        raw_data_gbif_path=Input(type="uri_file", path=raw_data_gbif_path),
        raw_data_species_path=Input(type="uri_folder", path=raw_data_species_path),
        species_file_name=species_file_name,
        columns_to_drop=columns_to_drop,
        drop_nans=drop_nans,
        target_col_name=target_col_name,
        correlation_id=str(correlation_id),
        parameters_path=Input(type="uri_file", path=parameters_path),
        train_fraction=train_fraction,
        random_seed=random_seed,
        number_of_folds=number_of_folds,
        maximum_evaluations=maximum_evaluations,
        hyperparameter_algorithm=hyperparameter_algorithm,
        verbose=verbose,
    )

    # demo how to change pipeline output settings
    pipeline_job.outputs.pipeline_job_prepped_data.mode = "rw_mount"

    # set pipeline level compute
    pipeline_job.settings.default_compute = cluster_name

    # set pipeline level datastore
    pipeline_job.settings.default_datastore = "workspaceblobstore"

    # Submit pipeline job to workspace
    pipeline_job = client.jobs.create_or_update(
        pipeline_job, experiment_name=experiment_name
    )

    if pipeline_job is None:
        azure_logger.event("Job creation failed")
        raise Exception("Sorry, Job creation failed")

    current_job = client.jobs.get(pipeline_job.name)
    if current_job is None:
        azure_logger.event("No job found with given name", **{"JobName": pipeline_job.name})
        raise Exception("Sorry, no job found with given name")

    if wait_for_completion == "True":
        total_wait_time = 3600
        current_wait_time = 0
        job_status = [
            "NotStarted",
            "Queued",
            "Starting",
            "Preparing",
            "Running",
            "Finalizing",
            "Provisioning",
            "CancelRequested",
            "Failed",
            "Canceled",
            "NotResponding",
        ]

        while current_job.status in job_status:
            if current_wait_time <= total_wait_time:
                time.sleep(15)
                current_job = client.jobs.get(pipeline_job.name)

                azure_logger.log("Job status, waiting", **{"Status": current_job.status})

                current_wait_time = current_wait_time + 15

                if current_job.status == "Failed":
                    break
            else:
                break

        azure_logger.log("Job status", **{"Status": current_job.status})

        if current_job.status == "Completed" or current_job.status == "Finished":
            azure_logger.event(
                "exiting job successfully", **{"job status": current_job.status}
            )
        else:
            azure_logger.event("exiting job with failure")
            raise Exception("Sorry, exiting job with failure..")


def main(argv) -> None:
    args = __define_arguments(argv)

    azure_logger = AzureLogger(correlation_id=args.correlation_id, level=logging.DEBUG)

    azure_logger.event(
        "starting mlops_pipelines",
        **{"site_name": args.site_name, "species_name": args.species_name},
    )
    start_time = time.time()
    try:
        mlops_pipeline(
            subscription_id=args.subscription_id,
            resource_group_name=args.resource_group_name,
            workspace_name=args.workspace_name,
            cluster_name=args.cluster_name,
            deploy_environment=args.deploy_environment,
            environment_name=args.environment_name,
            build_reference=args.build_reference,
            wait_for_completion=args.wait_for_completion,
            pipeline_pat=args.pipeline_pat,
            azdo_pipeline_rest_version=args.azdo_pipeline_rest_version,
            project_name=args.project_name,
            org_name=args.org_name,
            register_pipeline_definition_number=args.register_pipeline_definition_number,
            register_pipeline_version_number=args.register_pipeline_version_number,
            branch_name=args.branch_name,
            site_path=args.site_path,
            site_name=args.site_name,
            species_name=args.species_name,
            number_of_folds=args.number_of_folds,
            maximum_evaluations=args.maximum_evaluations,
            correlation_id=args.correlation_id,
        )

    except ArgumentException:
        sys.exit(2)
    except Exception as ex:
        azure_logger.exception(
            ex,
            "mlops_pipelines.py failed",
            **{"site_name": args.site_name, "species_name": args.species_name},
        )
        sys.exit(1)
    finally:
        end_time = time.time()
        azure_logger.event(
            "completed mlops_pipelines",
            **{
                "duration (s)": str((end_time - start_time)),
                "site_name": args.site_name,
                "species_name": args.species_name,
            },
        )


def __define_arguments(args):
    parser = argparse.ArgumentParser("mlops_pipeline")
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
        "--cluster_name",
        type=str,
        required=True,
        help="Azure Machine learning cluster name",
    )
    parser.add_argument(
        "--build_reference",
        type=str,
        required=True,
        help="Unique identifier for Azure DevOps pipeline run",
    )
    parser.add_argument(
        "--deploy_environment",
        type=str,
        required=True,
        help="execution and deployment environment. e.g. dev, prod, test",
    )
    parser.add_argument(
        "--wait_for_completion",
        type=str,
        required=True,
        help="determine if pipeline to wait for job completion",
    )
    parser.add_argument(
        "--pipeline_pat",
        type=str,
        required=True,
        help="pat for calling Azure DevOps Rest API authentication",
    )
    parser.add_argument(
        "--azdo_pipeline_rest_version",
        type=str,
        required=True,
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
        type=str,
        required=True,
        help="Azure DevOps pipeline version to use for callback",
    )
    parser.add_argument(
        "--register_pipeline_definition_number",
        type=str,
        required=True,
        help="Azure DevOps pipeline definition number to use for callback",
    )
    parser.add_argument(
        "--environment_name",
        type=str,
        required=True,
        help="Azure Machine Learning Environment name for job execution",
    )
    parser.add_argument(
        "--branch_name", type=str, required=True, help="Azure DevOps target branch name"
    )

    parser.add_argument(
        "--correlation_id",
        type=uuid.UUID,
        required=False,
        default=uuid.uuid4(),
        help="Application Insights correlation id if required.",
    )

    parser.add_argument(
        "--site_path",
        type=str,
        required=True,
        help="Storage path to the site specific folder to read data from.",
    )

    parser.add_argument(
        "--site_name", type=str, required=True, help="Name of the site being processed."
    )

    parser.add_argument(
        "--species_name",
        type=str,
        required=True,
        help="Name of the species being processed.",
    )

    parser.add_argument(
        "--number_of_folds",
        type=int,
        required=True,
        help="Required number of folds.",
    )

    parser.add_argument(
        "--maximum_evaluations",
        type=int,
        required=True,
        help="Maximum number of evaluations.",
    )

    return parser.parse_args(args)


if __name__ == "__main__":
    main(sys.argv[1:])
