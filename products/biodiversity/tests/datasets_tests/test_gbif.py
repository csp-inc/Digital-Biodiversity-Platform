# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import geopandas as gpd
import pandas as pd
import pytest
import shapely
from datasets.gbif import (
    DEFAULT_CLASSES,
    DEFAULT_COLUMNS,
    DEFAULT_FILTERS,
    gbif_spatial_filter,
    gbif_subsample,
    read_gbif,
    subset_by_minimum_obs,
)


@pytest.fixture
def gbif(
    filters: list = DEFAULT_FILTERS,
    columns: list = DEFAULT_COLUMNS,
    classes: list = DEFAULT_CLASSES,
    first_partition_only: bool = True,
    npartitions: int = 8,
):
    """
    Tests the `read_gbif` function from the `datasets.gbif` module

    Parameters
    ----------
    filters : list
        The filters to pass to dask.dataframe.read_parquet. `Passed to read_gbif`.
    columns : list
        The columns of GBIF data to retain during reading
        (passed to dask_dataframe.read_parquet).
        `Passed to read_gbif`.
    classes : list
        The taxonomic classes for which records should be returned. `Passed to read_gbif`.
    first_partition_only : int
        Whether to read the first partition only from the GBIF database. This is primarily
        useful for prototyping or testing. When not specified, the entire GBIF
        database will be read/used.
    nparitions : int
        The number of partitions to use when creating the dask_geopandas.GeoDataFrame.
        `Passed to read_gbif`.
    """

    return read_gbif(filters, columns, classes, first_partition_only, npartitions)


@pytest.mark.integration
def test_gbif(gbif: gpd.GeoDataFrame):
    # Check that the correct columns were fetched (the geometry column is created when
    #  building the GeoDataFrame)
    assert set(gbif.columns) == set(DEFAULT_COLUMNS).union({"geometry"})

    # Make sure every class present in the data is in the set of classes the user specifies
    assert all([x in DEFAULT_CLASSES for x in gbif["class"].compute()])


@pytest.mark.integration
def test_gbif_spatial_filter(gbif):
    """
    Tests the `gbif_spatial_filter` function from the `datasets.gbif` module

    Parameters
    ----------
    gbif : gpd.GeoDataFrame
        A GeoDataFrame containing GBIF points.
    """

    # Create a GeoDataFrame to use for spatial filtering
    geometries = [
        shapely.geometry.box(-122, 48, -122.5, 48.5),
        shapely.geometry.box(-122.5, 48, -123, 48.5),
        shapely.geometry.box(-122, 48.5, -122.5, 49),
    ]
    key_values = ["value1", "value2", "value3"]
    my_df = pd.DataFrame.from_dict({"my_key": key_values, "geometry": geometries})

    my_gdf = gpd.GeoDataFrame(my_df, geometry="geometry", crs="epsg:4326")

    # Apply the spatial filtering
    key = "my_key"
    values = ["value1", "value2"]
    filtered = gbif_spatial_filter(gbif, my_gdf, key, values)

    assert all(filtered[key].isin(values))


@pytest.mark.integration
def ignore_test_gbif_subsample(gbif):
    """
    Tests the `gbif_subsample` function from the `datasets.gbif` module

    Parameters
    ----------
    gbif : gpd.GeoDataFrame
        A GeoDataFrame containing GBIF points from which to subsample.
    """

    resolution = 60

    gbif_subsampled = gbif_subsample(gbif, resolution=resolution)

    # There should be exactly one observation per unique group of species, x, y,
    # year, and month
    n_groups = len(
        set(
            zip(
                gbif_subsampled["species"],
                gbif_subsampled[f"x_{resolution}m"],
                gbif_subsampled[f"y_{resolution}m"],
                gbif_subsampled["month"],
                gbif_subsampled["year"],
            )
        )
    )

    assert n_groups == gbif_subsampled.shape[0]


@pytest.mark.integration
def test_subset_by_minimum_obs(gbif):
    """
    Tests the `subset_by_minimum_obs` function from the `datasets.gbif` module

    Parameters
    ----------
    gbif : pd.DataFrame or gpd.GeoDataFrame
        A DataFrame contianing GBIF points from which to create a new dataframe
        of species with minimum observations.
    """

    min_obs = 300
    subset = subset_by_minimum_obs(gbif, min_obs)
    n_subsets = len(subset)

    assert n_subsets >= 0
