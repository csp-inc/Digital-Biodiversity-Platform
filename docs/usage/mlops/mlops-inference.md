# MLOps - Inference Pipeline

[[_TOC_]]

This document explains the functionality of the inference pipeline, which performs the following:

- Invoke batch endpoint for each species in a given list
- Wait for batch scoring to complete on each endpoint
- Download batch scoring results for each species
- Assemble all results into a single CSV and save to output location

## Code Structure

```tree
|
|-- .pipelines
|   |-- mlops
|       |-- execute_inference.yml
|-- products
|       |-- biodiversity
|           |-- inference
|               |-- azureml
|                   |-- inference.yml
|               |-- src
|                   |-- merge_data.py
|                   |-- perform_inference.py
|                   |-- sql_export.py
```

## Pipeline Flow

The inference pipeline is triggered manually in Azure DevOps. It is called `[MLOps] Inference` and the definition can be found under `.pipelines/mlops/execute_inference.yml`. This Azure DevOps Pipeline takes some parameters which are used to perform the inference:

| Name                        | Description                                | Example                            |
|-----------------------------|--------------------------------------------|------------------------------------|
| cherry_pt_segments          | Path to the cherry points segments file to use for inference. References to azureml data store files are preferred. | azureml://datastores/datablobstore/paths/cherrypt/configuration/cherry_pt_segments_60m_60m_2013_2021.csv |
| gold_table_path             | Path to the gold table csv file to use for inference. Named Azure ML Data assets are accepted, as well as references to azureml data store files.        | azureml:cherrypt-inference_features:1                               |
| site_name                   | Site name, the batch endpoints must be prefixed with this name, also used to create a folder to store batch scoring output.         | cherrypt                              |
| species_list_csv            | CSV file with list of species to perform inference for. The name of the species must be in the column called "species" and the batch endpoints must correspond to the naming convention `{site_name}-{species_name}` (cut off at 32 characters).        | azureml://datastores/datablobstore/paths/cherrypt/configuration/candidate_species.csv                               |
| max_wait_time               | Maximum wait time to wait for completion of single batch job in seconds         | 3600                            |
| inference_results           | Base path to folder where inference results should be stored. References to azureml data store folders are preferred.      | azureml://datastores/datablobstore/paths/cherrypt/results                              |

Some additional parameters are taken directly from the configured variable groups `MLOPS-CORE-VG` and `PLATFORM-DEV-VG`

- $AZURE_RM_SVC_CONNECTION
- $core_resource_group_name
- $core_machine_learning_workspace_name
- $core_subscription_id
- $core_key_vault_name

The ADO pipeline will in turn trigger the Azure ML Pipeline. The definition of this pipeline can be found under `products/biodiversity/inference/azureml/inference.yml`. The ADO pipeline will complete as soon as the AML has been triggered and will not wait for its completion, nor execute any callback.

The parameters given in ADO are passed to the AML Pipeline pipeline. Some parameters in the AML are only passed between the pipeline steps and are therefore populated automatically. For example, the outputs of `merge_data` are used as input for `perform_inference`. These folders are generated automatically by Azure ML under a new folder based on the following templatized path: {settings.datastore}/azureml/{job-name}/{output-name}/.

Within the AML Pipeline, the scripts `merge_data` and `perform_inference` are executed, which can all be found in the folder `products/biodiversity/inference/src`. The step `perform_inference` will invoke a batch endpoint for each species in the given species_list_csv file, wait for all batch jobs to complete, download their outputs and consolidate the results into a single csv file.

## Merge Data

The `merge_data` step merges data from the segments and the gold table into a single data frame which can be used as input for the batch scoring. A second data frame is also created from the merged data, containing some additional columns. This is later used as a basis for the final consolidated inference results file.

## Perform Inference

This is the heart of the inference pipeline. First, all species names are read from the species_list_csv file. The names of the species must be in the column "species". Later, the batch endpoints that are called must conform to the naming convention `{site_name}-{species_name}` (cut off at 32 characters). The script then invokes the batch endpoint for each species, using the batch input folder from the `merge_data` step as input. It is important that the batch_input_path parameter is given in the mode `direct`, otherwise the script will think this is a local folder and attempt to upload it to a AML datastore, which will most likely fail. In direct mode, the datastore path is passed as-is to the batch endpoint operation, which can download the data directly from the datastore, without the unncessary upload.

After all batch endpoints have been invoked, the script will now wait for all batch jobs to complete. Since the jobs are running in parallel, there is no need to use async to do this, but just sequentially query each job until it is done. Once the jobs are completed, every batch output file is downloaded to the local folder `{site_name}/{species_name}`. Since the downloaded predictions.csv is not a normal csv file according to the documentation, it cannot be read directly using pandas dataframes. Instead it is first modified to resemble a python list, which can then be transformed into a pandas dataframe. The column "predictions_PRESENT" are then added to the final inference results file, which is then saved in the specified inference_results output folder.

## Key Vault

The scripts require the name of an Azure Key Vault as a parameter so that they can read secrets from this key vault:

- APPLICATIONINSIGHTS-CONNECTION-STRING
- PC-SDK-SUBSCRIPTION-KEY

These secrets are saved as environment variables to be used by the pipeline scripts.

## Observe Pipelines

The pipeline runs can be observed in Azure DevOps and Azure Machine Learning. It is important to note that the ADO pipeline will complete successfully as soon as the AML pipeline is started. The ADO pipeline will not wait for the outcome of the AML pipeline, nor execute any callback. The AML pipeline can be observed in AML studio under the tab "Jobs". The inference pipeline itself and every single batch endpoint invocation will have their own entry in this list.

## Metrics and Logs

The inference pipeline sends metrics, logs and telemetry to App Insights. Logs can be observed in the Azure Portal in App Insights under "Logs" and "Events". For Kusto queries, the relevant tables are "traces" and "customEvents".
