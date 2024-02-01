# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

# Preprocess GBIF data and save to blob storage
# This script uses the functionality implemented in src/gbif.py to read, filter,
# and subsample the GBIF data. Specifically, this code retinas only observations
# that fall within the defined ecoregions.
# Subsampling also removes any spatiotemporal redundancy in the data.
# Importantly, this query only includes observations for **vertabrate species**.
# Given that this is primarily a feasibility study, reduced the groups of species
# for which we develop models will allow us to more rapidly iterate and evaluate
# the feasbility of our approach.
import argparse
import json
import logging
import sys
import time
import uuid

import dask_geopandas as dask_gpd
import datasets.gbif as gbif
import geopandas as gpd
import utils.dask_utils as dask
import utils.key_vault_utils as key_vault_utils
from argument_exception import ArgumentException
from azure_logger import AzureLogger


def preprocess_gbif(
    ecoregions_file_path: str,
    output_file_path: str,
    parameters_string: str,
    spatial_filter_projection_code: str,
    correlation_id: uuid.UUID = uuid.uuid4(),
    npartitions: int = 256,
    first_partition_only: bool = False,
    resolution: int = 60,
) -> None:
    azure_logger = AzureLogger(
        correlation_id=correlation_id,
        level=logging.DEBUG,
    )

    try:
        parameters = json.loads(parameters_string)
        values = json.loads(parameters["values"])

        # stateprovince to use in red_parquet filters
        stateprovince = parameters["stateprovince"]
        filters = []
        filters.extend(gbif.DEFAULT_FILTERS)
        filters.append(("stateprovince", "==", stateprovince))

        # Read in the GBIF data with dask, putting the result into
        # a dask.GeoDataFrame with 256 partitions
        npartitions = 256
        azure_logger.event("read_gbif", **{"npartitions": str(npartitions)})
        gbif_gdf = gbif.read_gbif(
            filters=filters,
            npartitions=npartitions,
            correlation_id=correlation_id,
            first_partition_only=first_partition_only,
        )

        # Read the ecoregions file.
        azure_logger.event(
            "reading ecoregions", **{"ecoregions_file_path": ecoregions_file_path}
        )

        file = open(ecoregions_file_path, "rb")

        # epsg:4326 is the EPSG code for the projection for Latitude/Longitude by the
        # geopandas library. Please refer to the documentation
        # here: https://geopandas.org/en/stable/docs/user_guide/projections.html
        ecoregions = gpd.read_file(file).to_crs("epsg:4326")

        azure_logger.log(
            "ecoregions read complete",
            **{
                "input_file_path": ecoregions_file_path,
                "size": str(ecoregions.size),
                "shape": str(ecoregions.shape),
            },
        )

        # Spatially filter the data to just the ecoregion areas.
        # This projection code is only valid for North America!
        azure_logger.event("apply spatial filter")
        gbif_filtered: dask_gpd.GeoDataFrame
        gbif_filtered = gbif.gbif_spatial_filter(
            gbif_gdf, ecoregions, key=parameters["key"], values=values
        ).to_crs(spatial_filter_projection_code)

        # Subsample the spatially filtered GBIF data to remove spatiotemporal
        # redundancies and reduce sample size
        azure_logger.event("subsample", **{"resolution": resolution})
        gbif_subsampled = gbif.gbif_subsample(gbif=gbif_filtered, resolution=resolution)

        # Write the subsampled data to a csv file.
        azure_logger.event("write to csv", **{"output_file_path": output_file_path})
        gbif_subsampled.to_csv(
            output_file_path,
        )
        azure_logger.event("write complete")

    except Exception as ex:
        azure_logger.exception(ex, "preprocess_gbif failed")
        raise


def main(argv):
    args = __define_arguments(argv)

    if args.key_vault_name is not None:
        key_vault_utils.get_environment_from_kv(args.key_vault_name)

    azure_logger = AzureLogger(
        correlation_id=args.correlation_id,
        level=logging.DEBUG,
    )

    azure_logger.event("starting preprocess_gbif")

    start_time = time.time()

    try:
        dask.initialize_dask_cluster(correlation_id=args.correlation_id)

        preprocess_gbif(
            args.ecoregions_file_path,
            args.output_file_path,
            args.parameters_string,
            args.spatial_filter_projection_code,
            args.correlation_id,
        )
    except ArgumentException:
        sys.exit(2)
    except Exception as ex:
        azure_logger.exception(ex, "preprocess_gbif.py failed")
        sys.exit(1)
    finally:
        end_time = time.time()

        azure_logger.event(
            "completed preprocess_gbif",
            **{"duration (s)": str((end_time - start_time))},
        )


def __define_arguments(args):
    parser = argparse.ArgumentParser("preprocess_gbif")

    parser.add_argument(
        "--ecoregions_file_path",
        required=True,
        type=str,
        help="Path to the ecoregions file.",
    )

    parser.add_argument(
        "--output_file_path",
        required=True,
        type=str,
        help="Path where the subsampled gbif output will be written.",
    )

    parser.add_argument(
        "--parameters_string",
        required=True,
        type=str,
        help="JSON encoded string of additional parameters.",
    )

    parser.add_argument(
        "--spatial_filter_projection_code",
        required=True,
        type=str,
        help='A valid spatial filter projection code. Must be of the form "epsg:{value}",',
    )

    parser.add_argument(
        "--correlation_id",
        type=uuid.UUID,
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

    return parser.parse_args(args)


if __name__ == "__main__":
    main(sys.argv[1:])
