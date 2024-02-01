# Data Preparation - Pipelines Configuration

[[_TOC_]]

## Execution in Azure ML

Each data preparation script has an associated Azure ML pipeline, in the form of a YAML file in the repo. The Azure ML YAML files are located at this location:

`products/biodiversity/dataprep/azureml`

### Example pipeline definition

Below is a simplified pipeline definition, to show the general structure of the YAML document. Here are the notable sections:

- `settings.default_compute` indicates which Azure ML compute cluster to use by default for all the jobs in the pipeline.
- `jobs` is a section that can contain multiple job definitions; in this example, there is only one job.
- `inputs` defines the inputs that will be passed to the job; these can be strings or numbers, but more importantly, they can point to files or folders in the data store. These files or folders will automatically be made available by Azure ML to the script as local files. You can learn more about [Data concepts in Azure Machine Learning](https://learn.microsoft.com/en-us/azure/machine-learning/concept-data) in the documentation.
- `outputs` similarly defines where the output should go.
- `environment` points to the Azure ML environment definition, which contains information like the base Docker image to use, the Python dependencies to install, etc. In this case, we use the `dask-mpi` environment.
- `distribution` and `resources` describe how to execute the script; in this example, we are requesting an MPI cluster of 12 nodes, with 8 Python instances per node (because we are using 8-core Virtual Machines).

```yaml
$schema: https://azuremlschemas.azureedge.net/latest/pipelineJob.schema.json
type: pipeline

description: Data preparation pipeline

display_name: test-pipeline
experiment_name: test-pipeline

settings:
  default_compute: azureml:clu-data-preparation-8fpz

jobs:

  demo-job:
    type: command

    inputs:
        inputfile:
            type: uri_file
            path: azureml://datastores/datablobstore/paths/foo/bar.xsc
        parameter1: 'test'

    outputs:
        outputfile:
            type: uri_file
            path: azureml://datastores/datablobstore/paths/output/file.csv

    code: .

    environment: azureml:dask-mpi@latest

    # Define the size of the MPI cluster
    distribution:
        type: mpi
        process_count_per_instance: 8
    resources:
        instance_count: 12

    # The command to execute
    command: >-
        python script.py --input ${{inputs.inputfile}} --output ${{outputs.outputfile}} --parameter '${{inputs.parameter1}}'
```

You can learn more about Azure ML Pipelines in the documentation: [Create and run machine learning pipelines using components with the Azure Machine Learning CLI](https://learn.microsoft.com/en-us/azure/machine-learning/how-to-create-component-pipelines-cli).

## List of pipelines

Each YAML file defines the input and output locations for the data files in Blob storage, using Azure ML URIs that reference a Datastore. For example, all paths can be prefixed with:

`azureml://datastores/datablobstore/paths`

Meaning that the files are available in a Blob Datastore named `datablobstore`.

They also define the runtime environment to use to run the script, either a normal Virtual Machine or an MPI/Dask cluster.

The following table summarizes these different configurations.

- `dataprep/azureml/gbif-pipeline.yml` runs `preprocess_gbif.py`
  - Inputs
    - `keyvault_name` (string), example: `keyvault_name`
    - `us_eco_l3` (file), example: `/epa_ecoregions/us_eco_l3.zip`
    - `filter` (string), example: `{"key":"US_L3NAME","values":"[\"Puget Lowland\",\"North Cascades\"]","stateprovince":"Washington"}`
    - `crs` (string), example: `epsg:26910`
  - Outputs
    - `gbif_subsampled` (file), example: `/cherrypt/training/gbif/gbif_subsampled.csv`
  - Runtime:
    - Dask/MPI
    - 12 nodes in the data prep cluster
  - Command: `preprocess_gbif.py --key_vault_name ${{inputs.keyvault_name}} --ecoregions_file_path ${{inputs.us_eco_l3}} --parameters_string '${{inputs.filter}}' --spatial_filter_projection_code  '${{inputs.crs}}' --output_file_path ${{outputs.gbif_subsampled}}`

- `dataprep/azureml/features-pipeline.yml` runs `feature_aggregation.py`, via `feature-component.yml`
  - Inputs
    - `keyvault_name` (string), example: `keyvault_name`
    - `gbif_subsampled` (file), example: `/gbif/gbif_subsampled.csv`
    - `args` (string), example: `--inference` to run feature aggregation for inference, or leave the default for training.
  - Outputs
    - `feature_xxx` (folder), one output per feature, pointing to the same directory, example: `/cherrypt/training/features`
  - Runtime:
    - Dask/MPI
    - 12 nodes in the data prep cluster
  - Component: `feature-component.yml`
    - To reduce repetition in the job definitions, a component YAML is used to run the script.
    - Command: `feature_aggregation.py --key_vault_name ${{inputs.keyvault_name}} --feature ${{inputs.feature}} --gbif_file ${{inputs.gbif_subsampled}} --output_path ${{outputs.features}} ${{inputs.args}}`

- `dataprep/azureml/candidate-species-pipeline.yml` runs `candidate_species_gbif.py`
  - Inputs
    - `keyvault_name` (string), example: `keyvault_name`
    - `gbif` (file), example: `/cherrypt/training/gbif/gbif_subsampled.csv`
    - `candidate_species_list` (file), example: `/cherrypt/configuration/candidate_species_list.csv`
  - Outputs
    - `species_gbif_output` (folder), example: `/cherrypt/training/species`
  - Runtime:
    - Single VM
  - Command: `candidate_species_gbif.py --key_vault_name ${{inputs.keyvault_name}} --gbif_vertebrates_file_path ${{inputs.gbif}} --candidate_species_list_path ${{inputs.candidate_species_list}} --csv_output_directory ${{outputs.species_gbif_output}}`

## List of secrets used

All jobs take an input named `keyvault_name`, that contains the name of the Key Vault that should be queried when running on Azure ML, to retrieve a number of secrets used by the scripts. When testing, it is possible to omit this parameter; in this case, the secrets need to be defined as environment variables. This would typically be used in the development environment.

- `APPLICATIONINSIGHTS-CONNECTION-STRING`: the connection string to the App Insights instance. Used by all scripts to send telemetry and logs.
- `PC-SDK-SUBSCRIPTION-KEY`: the Planetary Computer subscription key. Used by all scripts to lift API quotas.
