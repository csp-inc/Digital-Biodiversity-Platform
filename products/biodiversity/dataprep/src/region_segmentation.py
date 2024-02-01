# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import argparse
import logging
import sys
import time
import uuid
import warnings

import geopandas as gpd
import utils.key_vault_utils as key_vault_utils
import utm
from argument_exception import ArgumentException
from azure_logger import AzureLogger
from datasets.gnatsgo import convert_bbox_to_crs
from pyproj import CRS
from tools.segment_shp import get_spatiotemporal_segments


def region_segmentation(
    input_shapefile_path: str,
    output_csv_path: str,
    start_time: str,
    end_time: str,
    temporal_stride: str = "y",
    index_column_prefix: str = "CPT",
):
    gpd_region = gpd.read_file(input_shapefile_path)
    bbox_region = gpd_region.bounds

    # Ignore warning about inaccurate computation of centroid in WGS84
    warnings.filterwarnings("ignore", ".*Geometry is in a geographic CRS*.")
    lon, lat = gpd_region.centroid.x[0], gpd_region.centroid.y[0]
    _, _, zone, _ = utm.from_latlon(lat, lon)
    utm_epsg = CRS.from_dict({"proj": "utm", "zone": zone}).to_epsg()

    # Convert bounding box to UTM
    bbox = convert_bbox_to_crs(
        bbox_region.values[0], current_crs="EPSG:4326", new_crs=f"EPSG:{utm_epsg}"
    )

    region_segments = get_spatiotemporal_segments(
        bbox_utm=bbox,
        stride_x=60,
        stride_y=60,
        start_time=start_time,
        end_time=end_time,
        temporal_stride=temporal_stride,
        add_wgs84_coords=True,
        utm_epsg=utm_epsg,
        include_id_col=True,
        id_col_prefix=index_column_prefix,
    )

    region_segments.to_csv(output_csv_path)


def main(argv) -> None:
    args = __define_arguments(argv)

    if args.key_vault_name is not None:
        key_vault_utils.get_environment_from_kv(args.key_vault_name)

    azure_logger = AzureLogger(correlation_id=args.correlation_id, level=logging.DEBUG)

    azure_logger.event("starting region_segmentation")
    start_time = time.time()
    try:
        region_segmentation(
            input_shapefile_path=args.input_shapefile_path,
            output_csv_path=args.output_csv_path,
            start_time=args.start_time,
            end_time=args.end_time,
            temporal_stride=args.temporal_stride,
            index_column_prefix=args.index_column_prefix,
        )

    except ArgumentException:
        sys.exit(2)
    except Exception as ex:
        azure_logger.exception(ex, "region_segmentation failed")
        sys.exit(1)
    finally:
        end_time = time.time()
        azure_logger.event(
            "completed region_segmentation",
            **{"duration (s)": str((end_time - start_time))},
        )


def __define_arguments(args):
    parser = argparse.ArgumentParser("candidate_species_gbif")

    parser.add_argument(
        "--input_shapefile_path",
        type=str,
        required=True,
        help="Path, including filename, to the input Shapefile.",
    )
    parser.add_argument(
        "--output_csv_path",
        type=str,
        required=True,
        help="Path, including filename, where the output CSV file is written",
    )
    parser.add_argument(
        "--start_time",
        type=str,
        required=True,
        help="Start time.",
    )
    parser.add_argument(
        "--end_time",
        type=str,
        required=True,
        help="End time.",
    )
    parser.add_argument(
        "--temporal_stride",
        type=str,
        required=False,
        help="Whether to use temporal_stride, defaults to y",
    )
    parser.add_argument(
        "--index_column_prefix",
        type=str,
        required=False,
        help="Index column prefix, defaults to CPT",
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
    return parser.parse_args(args)


if __name__ == "__main__":
    main(sys.argv[1:])
