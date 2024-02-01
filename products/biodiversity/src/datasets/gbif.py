# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import logging
import os
import uuid

import backoff
import dask.array as da
import dask.dataframe as dd
import dask_distance
import dask_geopandas as dask_gpd
import geopandas as gpd
import numpy as np
import pandas as pd
import planetary_computer as pc
from azure_logger import AzureLogger
from pystac_client import Client
from pystac_client.exceptions import APIError

plantary_computer_uri = "https://planetarycomputer.microsoft.com/api/stac/v1"

DEFAULT_FILTERS = [
    ("year", ">=", 2014),
]

DEFAULT_COLUMNS = [
    "gbifid",
    "kingdom",
    "phylum",
    "class",
    "order",
    "family",
    "genus",
    "species",
    "occurrencestatus",
    "decimallatitude",
    "decimallongitude",
    "day",
    "month",
    "year",
    "dateidentified",
    "basisofrecord",
]
DEFAULT_CLASSES = ["Aves", "Mammalia", "Reptilia", "Amphibia"]

# The source quality rank serves to prioritize which records to keep in any
# given spatiotemporal group when subsampling. Entries with higher source
# quality ranks (based on the `basisofrecord` column) are prioritized for retention.
# This is not essential since the only observation types we have are preserved specimen
# and human observation, but I have included it in case it is needed for later
# (particularly if we model plants and insects).
SOURCE_QUALITY_RANKS = {
    "PRESERVED_SPECIMEN": 9,
    "FOSSIL_SPECIMEN": 1,
    "MATERIAL_SAMPLE": 7,
    "HUMAN_OBSERVATION": 9,
    "MACHINE_OBSERVATION": 8,
    "OCCURRENCE": 6,
    "MATERIAL_CITATION": 7,
    "LIVING_SPECIMEN": 9,
}


def get_read_gbif_max_retry_time() -> int:
    return int(os.getenv("READ_GBIF_MAX_RETRY_TIME", "300"))


@backoff.on_exception(backoff.expo, APIError, max_time=get_read_gbif_max_retry_time)
def read_gbif(
    filters: list = DEFAULT_FILTERS,
    columns: list = DEFAULT_COLUMNS,
    classes: list = DEFAULT_CLASSES,
    first_partition_only: bool = False,
    npartitions: int = 100,
    correlation_id: uuid.UUID = uuid.UUID(int=0),
) -> dask_gpd.GeoDataFrame:
    """
    Reads the GBIF data from the Planetary Computer, applies user-specified
    filters, and converts the result into a GeoDataFrame. Ensure you have a
    dask cluster running prior to using this function so it is run in parallel.

    Parameters
    ----------
    filters : list
        The filters to pass to dask.dataframe.read_parquet.
    columns : list
        The columns of GBIF data to retain during reading
        (passed to dask_dataframe.read_parquet).
    classes : list
        The taxonomic classes for which records should be returned.
        If `None` no class filter is applied.
    first_partition_only : bool
        Whether to read the first partition only from the GBIF database. This is primarily
        useful for prototyping or testing. When not specified, the entire GBIF
        database will be read/used.
    nparitions : int
        The number of partitions to use when creating the dask_geopandas.GeoDataFrame

    Returns
    -------
    GeoDataFrame
        A dask_geopandas.GeoDataFrame with GBIF records.
    """
    azure_logger = AzureLogger(correlation_id=correlation_id, level=logging.DEBUG)

    catalog = Client.open(plantary_computer_uri)
    gbif_latest = pc.sign(
        list(catalog.search(collections=["gbif"]).get_all_items())[0]
    ).assets["data"]

    azure_logger.log("read_parquet", **{"url": gbif_latest.href})

    # Initialize the dask.dataframe with lazy loading
    gbif_df = dd.read_parquet(  # pyright: reportPrivateImportUsage=false
        gbif_latest.href,
        filters=filters,
        columns=columns,
        storage_options=gbif_latest.extra_fields["table:storage_options"],
        parquet_file_extension=None,
        split_row_groups=True,
    )

    azure_logger.log(
        "gbif_df", **{"ndim": str(gbif_df.ndim), "npartitions": str(gbif_df.npartitions)}
    )

    if first_partition_only:
        azure_logger.log("first_partition_only")
        gbif_df = gbif_df.get_partition(0)

    if classes is not None:
        azure_logger.log("classes", **{"classes": classes})
        df_subset = (
            gbif_df[gbif_df["class"].isin(classes)]
            .dropna(subset=["species", "month", "day"])
            .compute()
        )
    else:
        azure_logger.log("no classes defined")
        df_subset = gbif_df.dropna(subset=["species", "month", "day"]).compute()

    # Convert the dataframe to a GeoDataFrame
    azure_logger.log("convert dataframe to GeoDataFrame")
    gdf = gpd.GeoDataFrame(
        df_subset,
        geometry=gpd.points_from_xy(df_subset.decimallongitude, df_subset.decimallatitude),
    ).set_crs("epsg:4326")

    return dask_gpd.from_geopandas(gdf, npartitions=npartitions)


def gbif_spatial_filter(
    gbif: gpd.GeoDataFrame,
    regions: gpd.GeoDataFrame,
    key: str = "US_L3NAME",
    values: list = ["Puget Lowland", "North Cascades"],
) -> gpd.GeoDataFrame:
    """
    Filters the GBIF data to only include points that fall within
    user-specified polygons in a user-provided GeoDataFrame. This function
    adds information from thee polygons to the GBIF data via a spatial join, and
    subsequently filters the points based on joined information. This defaults
    for this function assume that `regions` is a GeoDataFrame of US EPA ecoregions,
    but any GeoDataFrame (ideally with non-overlapping polygons) can be used.

    Parameters
    ----------
    gbif : geopandas.GeoDataFrame
        The GBIF dataset that will be filtered. Ideally initial filters
        will have already been applied in dask.dataframe.read_parquet.
    regions : geopandas.GeoDataFrame
        The polygons that will be used to spatially filter GBIF entries.
    key : str
        The key (column) in `regions` upon which to filter GBIF points.
        Note that changing this may also require changing `values`,
        as each key may have different possible values. More information
        on the possible columns upon which the filter can be based
        (if using US EPA Level III Ecoregions data) can be found here:
        https://gaftp.epa.gov/EPADataCommons/ORD/Ecoregions/us/Eco_Level_III_US.html.
    values : list
        The values to use for the filter based on key. Only records where key is
        equal to any element in values will be retained.

    Returns
    -------
    GeoDataFrame
        Filtered GBIF data with new columns from `regions` data added.
    """

    gbif_with_region = gbif.sjoin(regions, how="inner", predicate="intersects")

    return gbif_with_region[gbif_with_region[key].isin(values)]


def gbif_subsample(
    gbif: dask_gpd.GeoDataFrame, resolution: int = 60, crs: str = "epsg:26910"
) -> gpd.GeoDataFrame:
    """
    Subsamples the GBIF data to remove spatiotemporally redundant records.
    Only one record will be retained per unique species, coarsened spatial location,
    month, and yeah group.

    Parameters
    ----------
    gbif : dask_geopandas.GeoDataFrame
        The GBIF dataset that will be subsampled. Ideally initial filters
        will have already been applied in `read_gbif` and gbif_spatial_filter.
    resolution : int
        The resolution in units of `crs` (ideally meters) to use for temporarily
        coarsening the resolution for the purpose of subsampling.
    crs : str
        The coordinate reference system to use. Should be a projected coordinate system
        (i.e. within units in meters) and in the format `epsg:<epsg code>`.

    Returns
    -------
    GeoDataFrame
        The subsampled GBIF data
    """

    if f"epsg:{gbif.crs.to_epsg()}" != crs:
        gbif = gbif.to_crs(crs)

    # Add relative quality as a field based on basisofrecord
    gbif["source_quality_rank"] = gbif.basisofrecord.map(
        lambda x: SOURCE_QUALITY_RANKS[x], meta=("source_quality_rank", int)
    )

    # Add cordinates and coarsened coordinates as columns
    gbif["x"] = gbif.geometry.map(lambda x: x.x, meta=("x", float))
    gbif["y"] = gbif.geometry.map(lambda x: x.y, meta=("y", float))

    gbif[f"x_{resolution}m"] = gbif.x.map(lambda x: np.round(x / resolution) * resolution)
    gbif[f"y_{resolution}m"] = gbif.y.map(lambda x: np.round(x / resolution) * resolution)

    # Group the dataframe, extract the indices of the "best" observation from each group,
    # then subset the origina dataframe.
    # if we want to sort by quality and get the "best", we need an updated version of dask
    # but we cannot control the dask version used by the Planetary Computery cluster.
    # This is what to add once we get it working, and it requires dask>=2022.4.0
    # Needs to be done before the "groupby"
    # .sort_values(
    #     "source_quality_rank",
    #     ascending=False,
    #     npartitions=128
    # )
    # Implementing this now won't make any difference because all of
    # the observation have the same quality rank after our initial filtering
    gbif_subsample = (
        gbif.groupby(
            by=["species", "year", "month", f"x_{resolution}m", f"y_{resolution}m"]
        )
        .first()
        .compute()
        .reset_index()  # reset index to get group categories back as columns
    )

    return gbif_subsample


def subset_by_minimum_obs(df: pd.DataFrame, min_obs: int = 300) -> pd.DataFrame:
    """
    Subsets the GBIF data with a minimum number of observations within a species type.

    Parameters
    ----------
    df : pandas.DataFrame
        The GBIF dataset
    min_obs : int
        Minimum number of observations within a species type to be filtered by.

    Returns
    -------
    DataFrame
        DataFrame of species and observations that meet minimum number of
        observation criteria.
    """
    species, counts = np.unique(df.species, return_counts=True)
    species_sub = species[counts >= min_obs]
    counts_sub = counts[counts >= min_obs]
    subset_df = pd.DataFrame({"species": species_sub, "obs": counts_sub})
    return subset_df


def get_mean_sample_distance(
    df: pd.DataFrame, species: list, frac: float = 0.5
) -> pd.DataFrame:
    """
    Adds column to the GBIF data that denotest the mean distance within a list
    of species records.
    Note that the mean distance will be between species presence records of
    the same species.

    Parameters
    ----------
    df : pandas.DataFrame
        The GBIF dataset subsample to have a distance column added.
    species : list
        List of species in string format to calculate distance metric for.
    frac: float
        Proportion of data to create pairwise distance matrix from. Using full dataset
        can bog down dask cluster and overuse available memory.
    Returns
    -------
    DataFrame
        The GBIF data with added column for mean distance
    """
    # only need a subsample to get the distance measures to not be
    # memory limited by dask cluster
    sub_df = df[df["species"] == species].sample(frac=frac, replace=False, random_state=1)
    gdf = gpd.GeoDataFrame(
        sub_df,
        geometry=gpd.points_from_xy(sub_df.decimallongitude, sub_df.decimallatitude),
        crs=4326,
    )
    gdf = gdf.to_crs("epsg:26910")  # convert to mercator
    ed = fast_euclidean_solve(gdf)
    gdf["mean_euclidean_dist"] = ed  # add this value to the database
    return gdf


def fast_euclidean_solve(gdf: gpd.GeoDataFrame) -> np.ndarray:
    """
    From subset geodataframe of points, derives the pair-wise Euclidean distance matrix
    using dask-distance package

    Parameters
    ----------
    gdf : geopandas.GeoDataFrame
        The GBIF dataset subsample for a single species.

    Returns
    -------
    np.array
        The mean distance for each sample across all pairwise point combinations
    """
    p = da.from_array(
        [[x, y] for x, y in zip(gdf.geometry.values.x, gdf.geometry.values.y)],
        chunks=(1024, 1024),  # pyright: reportGeneralTypeIssues=false
    )
    dist = dask_distance.cdist(p, p, metric="euclidean")  # calculate the euclidean distance
    dist_array = da.mean(dist, axis=0)
    return dist_array.compute()
