# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import argparse
import logging
import os
import sys
import time
import uuid
from datetime import datetime
from typing import Optional, Union

import dask
import dask.dataframe as dd
import geopandas as gpd
import numpy as np
import pandas as pd
import utils.key_vault_utils as key_vault_utils
import xarray as xr
from azure_logger import AzureLogger
from chips_utils import (
    adaptive_grid_clustering,
    create_bbox_from_gbif_points,
    subset_xarr_by_xy_coords,
)
from datasets.terraclimate import preprocess_for_aggregation, transform_dataset
from onthefly_proc import (
    dask_gnatsgo_features_wrapper_onthefly,
    dask_index_features_wrapper_onthefly,
    dask_landsat8_features_wrapper_onthefly,
    dask_nasadem_features_wrapper_onthefly,
    dask_terraclimate_features_wrapper_onthefly,
    dask_water_features_wrapper_onthefly,
)
from rasterio.enums import Resampling
from stac_data_loader import STACDataLoader
from utils.dask_utils import initialize_dask_cluster

# Default Azure logger
azure_logger = AzureLogger(level=logging.DEBUG)


def execute_dask_procs(
    dask_procs: list,
    chktime_total: datetime,
    total_queued: int,
    current_queued: int,
    save_to_file: Optional[str] = None,
    storage_options: Optional[dict] = None,
):
    """
    Executes gathered dask processes, tracks progress and gathers results.

    Parameters
    ----------
    dask_procs : list of dask.delayed processes
        Contains processes to be executed.
    chktime_total : datetime.datetime
        Datetime object representing time of overall process starting.
        Serves only progress tracking purposes (calculate time per chip).
    total_queued : int
        The total number of queued processes so far (i.e. number of GBIF
        points queued in total).
    current_queued : int
        Number of points in current queued batch.
    save_to_file : str; default=None
        Path to file where results are saved (.csv). If None, does not save
        to file. If file at path already exists, appends data to it. Otherwise
        creates a new file.
    storage_options : dict; default=None
        Storage options for saving output to blob storage (ignored otherwise), e.g.
        account name, SAS token etc.

    Returns
    -------
    None
    """

    # execute processes
    azure_logger.log(
        "executing dask processes containing chips",
        **{"dask_procs": len(dask_procs), "current_queued_chips": current_queued},
    )

    dask_output = dask.compute(*dask_procs)  # pyright: reportPrivateImportUsage=false
    dask_procs = []
    total_time = (datetime.now() - chktime_total).total_seconds()
    azure_logger.log(
        "TOTAL: processed chips",
        **{"total_queued": total_queued, "s_per_schip": total_time / total_queued},
    )

    # gather results in dataframe
    df = pd.concat([item for item in dask_output])
    azure_logger.log(
        "coherence check", **{"current_queued": current_queued, "len_df": len(df)}
    )

    # save to file
    if save_to_file is not None:
        azure_logger.log("saving features to file", **{"save_to_file": save_to_file})
        df.to_csv(save_to_file, mode="w", storage_options=storage_options)

    return None


def collect_feature_settings(feature_type: str, custom_settings: bool = False):
    """
    Function collecting all the per-feature settings.

    Parameters
    ----------
    feature_type : str
        The feature type for which to return parameters.
    custom_settings : bool, default=None
        Set to True if custom settings are used.
    Returns
    -------
    A tuple of all the feature settings values.
    """
    temporal_aggregation = None
    spatial_agg_func = None
    temporal_agg_func = None
    index_formulas = None
    convert_to_reflectance = None
    dt_params = None
    perc_total_kwargs = None

    if not custom_settings:
        if feature_type == "landsat8":
            # query/process settings
            collection = ["landsat-c2-l2"]
            assets = [
                "coastal",
                "blue",
                "green",
                "red",
                "nir08",
                "swir16",
                "swir22",
                "lwir",
                "lwir11",
                "qa_pixel",
            ]
            resolution = 30
            query = {"eo:cloud_cover": {"lt": 50}, "platform": {"eq": "landsat-8"}}

            # aggregation settings
            temporal_aggregation = "seasonal"
            spatial_agg_func = "mean"
            temporal_agg_func = "median"
        elif feature_type == "indices":
            # query/process settings
            collection = ["landsat-c2-l2"]
            assets = [
                "coastal",
                "blue",
                "green",
                "red",
                "nir08",
                "swir16",
                "swir22",
                "lwir",
                "lwir11",
                "qa_pixel",
            ]
            resolution = 30
            query = {"eo:cloud_cover": {"lt": 50}, "platform": {"eq": "landsat-8"}}

            # aggregation settings
            index_formulas = {"ndvi": "([nir08]-[red]) / ([nir08]+[red])"}
            temporal_aggregation = "seasonal"
            spatial_agg_func = "mean"
            temporal_agg_func = "mean"
            convert_to_reflectance = True
        elif feature_type == "water":
            # query/process settings
            collection = ["landsat-c2-l2"]
            assets = ["blue", "green", "red", "nir08", "swir16", "swir22", "qa_pixel"]
            resolution = 30
            query = {"eo:cloud_cover": {"lt": 50}, "platform": {"eq": "landsat-8"}}

            # aggregation settings
            temporal_aggregation = "seasonal"
            spatial_agg_func = "perc_total"
            temporal_agg_func = "mean"
            dt_params = None
            perc_total_kwargs = None
        elif feature_type == "nasadem":
            # query/process settings
            collection = ["nasadem"]
            assets = ["elevation"]
            resolution = None
            query = None

            # no aggregation settings (hard-coded)

        elif feature_type == "gnatsgo":
            # query/process settings
            collection = ["gnatsgo-rasters"]
            assets = [
                "mukey",
                "aws0_100",
                "soc0_100",
                "tk0_100a",
                "tk0_100s",
                "musumcpct",
                "musumcpcta",
                "musumcpcts",
            ]
            resolution = None
            query = None

            # aggregation settings
            spatial_agg_func = "mean"

        elif feature_type == "terraclimate":
            # query/process settings
            collection = ["terraclimate"]
            assets = [
                "Q",
                "Ws",
                "Aet",
                "Def",
                "Pet",
                "Ppt",
                "Swe",
                "Vap",
                "Vpd",
                "Pdsi",
                "Soil",
                "Srad",
                "Tmax",
                "Tmin",
            ]
            resolution = None
            query = None

            # aggregation settings
            temporal_aggregation = "seasonal"
            spatial_agg_func = "mean"
            temporal_agg_func = "mean"
        else:
            raise ValueError(f"Feature type '{feature_type}' not recognized.")
    else:
        collection = None  # the collection(s) to load from Planetary Computer [list of str]
        assets = None  # the assets to load from the collection [list of str]
        # the resolution to resample to (same CRS as target collection)
        # [int/float or tuple of int/float]
        resolution = None
        query = None  # any additional query settings as a dictionary (JSON) [see above]

    return (
        collection,
        assets,
        resolution,
        query,
        temporal_aggregation,
        spatial_agg_func,
        temporal_agg_func,
        index_formulas,
        convert_to_reflectance,
        dt_params,
        perc_total_kwargs,
    )


def query_stac(
    collection: Optional[list],
    gdf_aoi: Union[gpd.GeoDataFrame, pd.DataFrame],
    start: datetime,
    end: datetime,
    assets: Optional[list],
    buffer_size: int,
    query: Optional[dict],
    retries: int = 5,
    wait_time: int = 5,
):
    """
    Wrapper to query the STAC API with retries.

    Parameters
    ----------
    retries: int, default=5
        Number of times to retry the call to the STAC API.
    wait_time: int, default=5
        Time in seconds to wait between retries.

    See the docstring for STACDataLoader for a description of all other parameters.

    Returns
    -------
    A tuple with the STACDataLoader, the query results, and a GeoDataframe of results.
    Will raise an Exception if the STAC API can not be contacted after a number of retries.
    """
    for _ in range(retries):
        try:
            stacdl = STACDataLoader(
                endpoint="https://planetarycomputer.microsoft.com/api/stac/v1",
                collections=collection,
                aoi=gdf_aoi,
                time_range=(start, end),
                assets=assets,
                spatial_buffer=1.5 * buffer_size,
                temporal_buffer=0,
                verbose=False,
                query=query,
                max_items=None,
            )
            results, query_gdf = stacdl.query()

            # Exit loop if OK
            return (stacdl, results, query_gdf)

        except Exception as ex:
            azure_logger.log("STACDataLoader failed")
            azure_logger.exception(ex)
            azure_logger.log(f"waiting {wait_time} s...")
            time.sleep(wait_time)

    raise (Exception(f"Could not contact STAC API after {retries} tries"))


def execute_spatial_grouping(gbif):
    """
    Spatial grouping

    Group GBIF points via adaptive grid clustering to optimize query performance and avoid
    excessive resource use. Ensures that any individual query does not load too large an
    area at once to limit the size of created xarrays.
    """
    # group points spatially via adaptive grid
    grid_counts, grid_coords = adaptive_grid_clustering(
        gbif[["decimallongitude", "decimallatitude"]],
        grid_size=(0.1, 0.1),
        min_cell_size=0.0001,
        max_cluster_size=50000,
    )

    # assign spatial clusters to dataframe and group temporally by year/month
    # temp function for assigning clusters to dataframe
    def _gbif_assign_grid_cluster(x, grid_coords):
        lon, lat = x
        for cluster, coords in grid_coords.items():
            if (
                lon > coords[0]
                and lon <= coords[2]
                and lat > coords[1]
                and lat <= coords[3]
            ):
                return cluster

    # process via Dask
    ddf = dd.from_pandas(gbif, npartitions=20)  # pyright: reportPrivateImportUsage=false
    ddf["cluster"] = ddf[["decimallongitude", "decimallatitude"]].apply(  # type: ignore
        _gbif_assign_grid_cluster, grid_coords=grid_coords, axis=1
    )
    gbif_with_clusters = ddf.compute()

    return gbif_with_clusters


def get_latest_file_number(folder_path: str):
    """
    This function gets the latest file number from folder

    Parameters
    ----------
    folder_path : str
        Relative path to the folder
        e.g. 'gbif/features/full'

    Returns
    -------
    latest_index: latest file number from folder
    """

    latest_index = -1
    blob_name_list = []

    source_blob_list = os.listdir(folder_path)
    for blob in source_blob_list:
        blob_number = blob.rsplit("_", 1)[1].rsplit(".", 1)[0]
        blob_name_list.append(int(blob_number))

    if len(blob_name_list) >= 1:
        latest_index = max(blob_name_list)

    return latest_index


def get_dask_delayed_process(
    feature_type,
    df,
    xarr,
    buffer_size,
    temporal_aggregation,
    spatial_agg_func,
    temporal_agg_func,
    index_formulas,
    convert_to_reflectance,
    dt_params,
    perc_total_kwargs,
):
    dask_proc = None

    # add delayed process
    if feature_type == "landsat8":
        dask_proc = dask.delayed(dask_landsat8_features_wrapper_onthefly)(
            gbif_items=df,
            xarr=xarr,
            buffer_size=buffer_size,
            temporal_aggregation=temporal_aggregation,
            spatial_agg_func=spatial_agg_func,
            temporal_agg_func=temporal_agg_func,
        )
    elif feature_type == "indices":
        dask_proc = dask.delayed(dask_index_features_wrapper_onthefly)(
            gbif_items=df,
            xarr=xarr,
            buffer_size=buffer_size,
            temporal_aggregation=temporal_aggregation,
            spatial_agg_func=spatial_agg_func,
            temporal_agg_func=temporal_agg_func,
            index_formulas=index_formulas,
            convert_to_reflectance=convert_to_reflectance,
        )
    elif feature_type == "water":
        dask_proc = dask.delayed(dask_water_features_wrapper_onthefly)(
            gbif_items=df,
            xarr=xarr,
            buffer_size=buffer_size,
            temporal_aggregation=temporal_aggregation,
            spatial_agg_func=spatial_agg_func,
            temporal_agg_func=temporal_agg_func,
            dt_params=dt_params,
            perc_total_kwargs=perc_total_kwargs,
        )
    elif feature_type == "nasadem":
        dask_proc = dask.delayed(dask_nasadem_features_wrapper_onthefly)(
            gbif_items=df,
            xarr=xarr,
            buffer_size=buffer_size,
        )
    elif feature_type == "gnatsgo":
        dask_proc = dask.delayed(dask_gnatsgo_features_wrapper_onthefly)(
            gbif_items=df,
            xarr=xarr,
            buffer_size=buffer_size,
            spatial_agg_func=spatial_agg_func,
        )
    elif feature_type == "terraclimate":
        dask_proc = dask.delayed(dask_terraclimate_features_wrapper_onthefly)(
            gbif_items=df,
            xarr=xarr,
            temporal_aggregation=temporal_aggregation,
            spatial_agg_func=spatial_agg_func,
            temporal_agg_func=temporal_agg_func,
        )

    return dask_proc


def feature_aggregation(
    feature_type: str,
    gbif_filepath: str,
    output_folder: str,
    spatial_grouping: bool = True,
    buffer_size: int = 2000,
    batch_size: int = 200,
    pprocs: int = 150,
    inference: bool = False,
):
    """
    Execute the feature aggregation.

    Parameters
    ----------
    ferature_type: str
        defines the feature type to produce: landsat8, nasadem, gnatsgo, water,
        indices, terraclimate
    gbif_filepath: str
        The path of the GBIF input file.
    output_folder: str
        The path of the folder to write results to.
    spatial_grouping: bool, default=True
        If True, applies spatial grouping to GBIF dataframe; if False, expects the
        'cluster' column to be already present in GBIF dataframe.
    buffer_size: int, default=500
        Spatial buffer size for STAC queries.
    batch_size: int, default=200
        Number of chips in each delayed Dask process.
    pprocs: int, default=150
        Number of parallel processes to aim for in each dask.delayed execution.

    Returns
    -------
    None
    """

    azure_logger.log("feature_aggregation args", **{"args": str(locals())})

    # collect feature settings
    (
        collection,
        assets,
        resolution,
        query,
        temporal_aggregation,
        spatial_agg_func,
        temporal_agg_func,
        index_formulas,
        convert_to_reflectance,
        dt_params,
        perc_total_kwargs,
    ) = collect_feature_settings(feature_type)

    # output path for the feature files
    output_path = f"{output_folder}/{feature_type}"

    # create the output folder
    os.makedirs(output_path, exist_ok=True)

    # read the input GBIF file
    gbif = pd.read_csv(gbif_filepath)

    # changes needed for segments dataframe
    if inference:
        gbif["gbifid"] = gbif["id"]
        gbif["year"] = gbif["time"].apply(lambda x: int(x.split("-")[0]))
        gbif["month"] = gbif["time"].apply(lambda x: int(x.split("-")[1]))

    # spatial grouping
    if spatial_grouping:
        gbif = execute_spatial_grouping(gbif)

    """
    Start of the aggregation process
    """

    chktime_total = datetime.now()
    azure_logger.log("aggregation process started", **{"time": str(datetime.now())})

    items_cluster_year = gbif[["cluster", "year"]].values.tolist()
    items_cluster_year = sorted(list(set(tuple(map(tuple, items_cluster_year)))))
    azure_logger.log(
        "Total number of cluster and year combinations",
        **{"items_cluster_year": len(items_cluster_year)},
    )

    # get the latest index of cluster, year combo
    latest_index = get_latest_file_number(output_path)
    azure_logger.log(
        "The latest item number in the Blob Storage", **{"latest_index": latest_index}
    )

    total_queued = 0
    current_queued = 0
    dask_procs = []
    save_features_to_file = None

    # spatial groups
    for index, cluster_year in enumerate(
        items_cluster_year[latest_index + 1 :], latest_index + 1
    ):
        azure_logger.log(
            "preparing cluster",
            **{
                "index": index,
                "items_cluster_year": len(items_cluster_year),
                "cluster_year": cluster_year,
            },
        )

        # file to save features to
        save_features_to_file = f"{output_path}/{feature_type}_{index}.csv"

        # temporal groups
        # for year in sorted(gbif_part['year'].unique()):
        cluster, year = cluster_year
        df_temp = gbif[(gbif.year == int(year)) & (gbif.cluster == cluster)]

        # only proceed if there are chips in spatiotemporal selection
        if len(df_temp) > 0:
            # determining start and end date for query depending on feature_type
            if feature_type == "nasadem":
                start = datetime(2000, 1, 1)
                end = datetime(2000, 12, 31)
            elif feature_type == "gnatsgo":
                start = datetime(2020, 1, 1)
                end = datetime(2020, 12, 31)
            elif feature_type == "terraclimate":
                start = datetime(int(year) - 10, 1, 1)
                end = datetime(int(year) - 1, 12, 31)
            else:
                start = datetime(int(year) - 1, 1, 1)
                end = datetime(int(year) - 1, 12, 31)

            df_temp = df_temp.reset_index(drop=True)

            # creating bounding box around points in current selection
            bbox = create_bbox_from_gbif_points(df_temp, as_polygon=True)
            gdf_aoi = gpd.GeoDataFrame({"geometry": [bbox]})
            gdf_aoi = gdf_aoi.set_crs("epsg:4326")

            # send query to Planetary Computer
            (stacdl, results, query_gdf) = query_stac(
                collection, gdf_aoi, start, end, assets, buffer_size, query
            )

            # get number of results for zarr (returned gdf) and non-zarr collections
            # (only signed items)
            if query_gdf is not None:
                n_results = len(query_gdf)
            else:
                n_results = len(stacdl.signed_items)

            # only proceed if there are files in query
            if n_results > 0:
                azure_logger.log(
                    "preparing cluster",
                    **{
                        "index": index,
                        "cluster": cluster,
                        "year": year,
                        "chips_in_curent_batch": len(df_temp),
                    },
                )
                azure_logger.log("Loading data into xarray.")

                # lazily initialize xarray
                xarr = stacdl.load_data(
                    resolution=resolution,
                    resampling=Resampling.nearest,
                    clip=False,
                    chunksize=(2, 16, 2048, 2048),
                )

                # specific preparation needed for TerraClimate zarr xarray
                if feature_type == "terraclimate":
                    xarr = xarr[0]
                    xarr = transform_dataset(xarr)  # type: ignore
                    xarr = preprocess_for_aggregation(xarr)

                # if the number of chips in selection is larger than batch_size,
                # split up into smaller parts
                azure_logger.log("Preparing dask processes.")
                for b in np.arange(0, len(df_temp), batch_size):
                    df_ = df_temp[b : b + batch_size]

                    # specific subsetting of xarray for TerraClimate
                    if feature_type == "terraclimate":
                        lons = np.array(df_["decimallongitude"].values)
                        lats = np.array(df_["decimallatitude"].values)
                        bbox = (np.min(lons), np.min(lats), np.max(lons), np.max(lats))
                        xarr_ = subset_xarr_by_xy_coords(
                            xarr, bbox, x_dim_name="x", y_dim_name="y"
                        )
                    else:
                        xarr_ = xarr

                    # make sure the xarr has time dimension > 0;
                    # this is needed to catch special case of empty TerraClimate xarray
                    min_dim_size = 0
                    if isinstance(xarr_, xr.DataArray):
                        min_dim_size = min(xarr_.shape)
                    elif isinstance(xarr_, xr.Dataset):
                        min_dim_size = min(xarr_.data.shape)

                    dask_proc = None
                    if min_dim_size > 0:
                        dask_proc = get_dask_delayed_process(
                            feature_type,
                            df_,
                            xarr_,
                            buffer_size,
                            temporal_aggregation=temporal_aggregation,
                            spatial_agg_func=spatial_agg_func,
                            temporal_agg_func=temporal_agg_func,
                            index_formulas=index_formulas,
                            convert_to_reflectance=convert_to_reflectance,
                            dt_params=dt_params,
                            perc_total_kwargs=perc_total_kwargs,
                        )

                        dask_procs.append(dask_proc)

                        total_queued += len(df_)
                        current_queued += len(df_)

                    # trigger dask process
                    if len(dask_procs) >= pprocs:
                        execute_dask_procs(
                            dask_procs,
                            chktime_total,
                            total_queued,
                            current_queued,
                            save_to_file=save_features_to_file,
                        )
                        dask_procs = []
                        current_queued = 0

                # trigger dask process
                if len(dask_procs) >= pprocs:
                    execute_dask_procs(
                        dask_procs,
                        chktime_total,
                        total_queued,
                        current_queued,
                        save_to_file=save_features_to_file,
                    )
                    dask_procs = []
                    current_queued = 0

            # trigger dask process
            if len(dask_procs) >= pprocs:
                execute_dask_procs(
                    dask_procs,
                    chktime_total,
                    total_queued,
                    current_queued,
                    save_to_file=save_features_to_file,
                )
                dask_procs = []
                current_queued = 0
        # trigger dask process
        if len(dask_procs) >= pprocs:
            execute_dask_procs(
                dask_procs,
                chktime_total,
                total_queued,
                current_queued,
                save_to_file=save_features_to_file,
            )
            dask_procs = []
            current_queued = 0
    # trigger dask process
    if len(dask_procs) >= 1:
        execute_dask_procs(
            dask_procs,
            chktime_total,
            total_queued,
            current_queued,
            save_to_file=save_features_to_file,
        )
        dask_procs = []
        current_queued = 0


def main():
    global azure_logger

    args = __define_arguments()

    if args.key_vault_name is not None:
        key_vault_utils.get_environment_from_kv(args.key_vault_name)

    # Azure logger with correlation ID
    azure_logger = AzureLogger(correlation_id=args.correlation_id, level=logging.DEBUG)

    try:
        initialize_dask_cluster(correlation_id=args.correlation_id)
        feature_aggregation(
            feature_type=args.feature,
            gbif_filepath=args.gbif_file,
            output_folder=args.output_path,
            spatial_grouping=args.spatial_grouping,
            buffer_size=args.buffer_size,
            batch_size=args.batch_size,
            pprocs=args.pprocs,
            inference=args.inference,
        )
    except Exception as ex:
        azure_logger.exception(ex)
        sys.exit(2)


def __define_arguments():
    parser = argparse.ArgumentParser("feature_aggregation")

    parser.add_argument(
        "--feature",
        required=True,
        type=str,
        choices=["landsat8", "indices", "water", "nasadem", "gnatsgo", "terraclimate"],
        help="The name of the feature to generate.",
    )
    parser.add_argument(
        "--gbif_file",
        required=True,
        type=str,
        help="Path of the input GBIF file.",
    )
    parser.add_argument(
        "--output_path",
        type=str,
        required=True,
        help="Path of the folder for output CSV files.",
    )
    parser.add_argument(
        "--spatial_grouping",
        required=False,
        action="store_const",
        const=False,
        default=True,
        help="Apply spatial grouping to GBIF dataframe.",
    )
    parser.add_argument(
        "--buffer_size",
        type=int,
        required=False,
        default=2000,
        help="Spatial buffer size for STAC queries.",
    )
    parser.add_argument(
        "--batch_size",
        type=int,
        required=False,
        default=200,
        help="Number of chips in each delayed Dask process.",
    )
    parser.add_argument(
        "--pprocs",
        type=int,
        required=False,
        default=150,
        help="Number of parallel processes to aim for in each dask.delayed execution.",
    )
    parser.add_argument(
        "--correlation_id",
        type=uuid.UUID,
        required=False,
        default=uuid.uuid4(),
        help="Application Insights correlation ID if required.",
    )
    parser.add_argument(
        "--inference",
        required=False,
        action="store_const",
        const=True,
        default=False,
        help="Perform feature aggregation for inference.",
    )

    parser.add_argument(
        "--key_vault_name",
        type=str,
        required=False,
        help="Key Vault name where to retrieve secrets.",
    )

    return parser.parse_args(sys.argv[1:])


if __name__ == "__main__":
    main()
