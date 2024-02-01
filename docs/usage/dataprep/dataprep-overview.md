# Data Preparation - Overview

This folder contains the documentation for the data preparation pipelines. This page is the Overview.

[[_TOC_]]

## Overview

The goal of the data preparation pipelines is to retrieve and prepare the data that will be used by the MLOps pipelines, for training the models and for inference.

## Pipelines

The diagram below shows the different data preparation pipelines and how they interact to create the data files. Each diamond box represents a data preparation script, encapsulated into an Azure ML pipeline.

You can read more details about the configuration of the Azure ML pipelines in [Pipelines Configuration](03-pipelines-configuration.md).

### Training

::: mermaid
graph TD
A1[us_eco_l3.zip] --> preprocess_gbif{preprocess_gbif.py}
A2(filter definition) --> preprocess_gbif
preprocess_gbif --> gbif_subsampled[gbif_subsampled.csv]

    preprocess_gbif -.-> planetary_computer[(Planetary Computer)]

    gbif_subsampled --> feature_aggregation{feature_aggregation.py}
    feature_aggregation --> feature1[gnatsgo/*.csv]
    feature_aggregation --> feature2[landsat8/*.csv]
    feature_aggregation --> feature3[nasadem/*.csv]
    feature_aggregation --> feature4[terraclimate/*.csv]
    feature_aggregation --> feature5[water/*.csv]

    feature_aggregation -.-> planetary_computer

    feature1 --> create_gold_table{create_gold_table.py}
    feature2 --> create_gold_table
    feature3 --> create_gold_table
    feature4 --> create_gold_table
    feature5 --> create_gold_table
    create_gold_table --> gold_table[gold_data_table.csv]

    gbif_subsampled --> candidate_species{candidate_species_gbif.py}

    candidate_species --> species_golddata["*_golddata.csv = 16 files, one per species"]

    gold_table --> train_model((MODEL TRAINING))

    species_list[candidate_species.csv] --> candidate_species

    species_golddata --> train_model

:::

### Inference

::: mermaid
graph TD

    gbif_subsampled[cherry_pt_segments_60m_60m_2013_2021.csv] --> feature_aggregation{feature_aggregation.py}
    feature_aggregation --> feature1[gnatsgo/*.csv]
    feature_aggregation --> feature2[landsat8/*.csv]
    feature_aggregation --> feature3[nasadem/*.csv]
    feature_aggregation --> feature4[terraclimate/*.csv]
    feature_aggregation --> feature5[water/*.csv]

    feature_aggregation -.-> planetary_computer[(Planetary Computer)]

    feature1 --> create_gold_table{create_gold_table.py}
    feature2 --> create_gold_table
    feature3 --> create_gold_table
    feature4 --> create_gold_table
    feature5 --> create_gold_table
    create_gold_table --> gold_table[cherry_pt_gold.csv]

    gold_table --> train_model((INFERENCE))

:::

## Data structure

This sections describes the overall structure of the data files created by the pipelines for a single site.

```shell
cherrypt/
├─ configuration/
│  ├─ candidate_species_list_500k.csv
│  ├─ candidate_species.csv
│  ├─ cherry_pt_segments_60m_60m_2013_2021.csv
│  ├─ us_eco_l3.zip
├─ training/
│  ├─ gbif/
│  │  ├─ gbif_subsampled.csv
│  ├─ features/
│  ├─ species/
│  │  ├─ species_1_golddata.csv
│  │  ├─ species_2_golddata.csv
│  │  ├─ species_n_golddata.csv
│  ├─ features_gold_data_table.csv
├─ inference/
│  ├─ features/
│  ├─ cherry_pt_gold.csv
```

Here is an explanation of each file.

- `cherrypt` is the "short name" for the site to which belongs all the data. This gives the possibility to create more sites by using different top level directories.
- `configuration` contains all the input data relative to the whole site.

  - `candidate_species_list_500k.csv` contains the list of species of interest for the site (used for data preparation).
  - `candidate_species.csv` contains the list of species of interest for the site (subset of previous one used for training models and inference).
  - `cherry_pt_segments_60m_60m_2013_2021.csv` is a list of spatiotemporal segements, or points, artificially created in the form of a grid, and designed to mimic the format of the GBIF file. This file is used to perform the inference on the site. It was retrieved from the Phase 1 data repository.
  - `us_eco_l3.zip` contains the definition of the US ecoregions file. It is used by the `gbif-pipeline`. It can be downloaded from the US EPA ecoregions site.

- `training` contains all the data used for training the models.

  - `gbif/gbif_subsampled.csv` contains all the GBIF observations used to perform the training. It is the output of the pipeline `gbif-pipeline`.
  - `features_gold_data_table.csv` contains all the aggregated features extracted per GBIF point. It is the output of the pipeline `features-pipeline`.
  - `species` contains one table of absent/present observations per species of interest. It is the output of the pipeline `candidate-species-pipeline`.

- `inference` contains all the data used for inference.
  - `cherry_pt_gold.csv` contains the aggregated features per geospatial segment over the site, based on the segements defined in the configuration. It is the output of the pipeline `inference-features-pipeline`.

The `features` folders contain the individual feature files, both for training and inference. These files are kept for reference only, since they are aggregated into "gold data" tables.

```shell
features/
├─ gnatsgo/
├─ indices/
├─ landsat8/
├─ nasadem/
├─ terraclimate/
├─ water/
```

> NOTE: When creating new data files to perform either training or inference, a new unique suffix should be added, so that the new data files do not overwrite the previous versions. For example:

```shell
cherrypt/
├─ training/
│  ├─ gbif/
│  │  ├─ gbif_subsampled_20230130.csv
│  ├─ features_20230130/
│  ├─ species_20230130/
│  │  ├─ species_1_golddata.csv
│  │  ├─ species_2_golddata.csv
│  │  ├─ species_n_golddata.csv
│  ├─ features_gold_data_table_20230130.csv
├─ inference/
│  ├─ features/
│  ├─ cherry_pt_gold_20230130.csv
```

The resulting file paths can then be used in the training or inference pipelines to use the new data.
