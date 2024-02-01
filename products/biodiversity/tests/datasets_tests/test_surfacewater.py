# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

""" Testcases for dataets/surfacewater.py

    Current implementation of surface water derivation operates over a raster
    at a fixed time point (3D vector)
"""

from datetime import datetime
from unittest import TestCase

import geopandas as gpd
import numpy as np
import pytest
from datasets.surfacewater import calculate_indicies, derive_surface_water_extent
from parameterized import parameterized
from shapely.geometry import Polygon
from stac_data_loader import STACDataLoader

# Sample diagnostic test parameters
dt_params = {
    # Test 1, 2, 3
    "wigt": 124,  # Wetness index threshold, default 0.124 (index values multipled by 1e5)
    "awgt": 0,  # Automated water extent shadow threshold
    # Test 4
    "pswt_1_mndwi": -4400,  # Partial surface water test-1 MNDWI threshold, default -4400
    "pswt_1_swir1": 9000,  # Partial surface water test-1 SWIR1 threshold, default 9000
    "pswt_1_nir": 15000,  # Partial surface water test-1 NIR threshold, default 15000
    "pswt_1_ndvi": 0.7,  # Partial surface water test-1 NDVI threshold
    # Test 5
    "pswt_2_mndwi": -5000,  # Partial surface water test-3 MNDWI threshold
    "pswt_2_blue": 1000,  # Partial surface water test-3 Blue threshold
    "pswt_2_nir": 2500,  # Partial surface water test-3 NIR threshold, default 2500
    "pswt_2_swir1": 3000,  # Partial surface water test-3 SWIR1 threshold
    "pswt_2_swir2": 1000,  # Partial surface water test-3 SWIR2 threshold
}


def example_aoi():
    aoi = Polygon(
        (
            (-21.22, 64.18),
            (-21.05, 64.18),
            (-21.05, 64.12),
            (-21.22, 64.12),
            (-21.22, 64.18),
        )
    )
    gdf_aoi = gpd.GeoDataFrame({"geometry": [aoi]}).set_crs(4326)
    return gdf_aoi


def create_landsat_test_case_1():
    settings = dict()
    settings["endpoint"] = "https://planetarycomputer.microsoft.com/api/stac/v1"
    settings["collections"] = ["landsat-c2-l2"]
    settings["aoi"] = example_aoi()
    settings["time_range"] = (datetime(2020, 1, 1), datetime(2021, 12, 31))
    settings["assets"] = "blue green red nir08 swir16 swir22 qa_pixel".split()
    settings["spatial_buffer"] = 1000
    settings["temporal_buffer"] = 0
    settings["query"] = {"eo:cloud_cover": {"lt": 50}}
    settings["max_items"] = None
    return settings


def create_landsat_test_case_2():
    settings = dict()
    settings["endpoint"] = "https://planetarycomputer.microsoft.com/api/stac/v1"
    settings["collections"] = ["landsat-c2-l2"]
    settings["aoi"] = example_aoi()
    settings["time_range"] = (datetime(2020, 1, 1), datetime(2020, 6, 1))
    settings["assets"] = "blue green red nir08 swir16 swir22 qa_pixel".split()
    settings["spatial_buffer"] = 1000
    settings["temporal_buffer"] = 0
    settings["query"] = {"eo:cloud_cover": {"lt": 50}}
    settings["max_items"] = None
    return settings


class TestSurfaceWater(TestCase):
    @parameterized.expand(
        [(create_landsat_test_case_1(),), (create_landsat_test_case_2(),)]
    )
    @pytest.mark.integration
    def test_band_presence(self, test_case):
        """
        Tests if the 7 required bands are present within STAC API response.
        """

        stacdl = STACDataLoader(**test_case)
        results, query_gdf = stacdl.query()
        raster_xr = stacdl.load_data(resolution=30)
        assert len(raster_xr.band) == 7

    @parameterized.expand([(create_landsat_test_case_2(),)])
    @pytest.mark.integration
    def test_derive_surface_water_extent(self, test_case):
        """
        Verifies outputs of surface water extent is within allowable range.
        This indiciates that all pixels were properly assigned a dswe category.
        """

        stacdl = STACDataLoader(**test_case)
        results, query_gdf = stacdl.query()
        raster_xr = stacdl.load_data(resolution=30)

        dswe = derive_surface_water_extent(raster_xr, dt_params)
        assert ((dswe.values >= 0) & (dswe.values <= 5)).all()

    @parameterized.expand([(create_landsat_test_case_2(),)])
    @pytest.mark.integration
    def test_calculate_indicies(self, test_case):
        """
        Check the output of calculate_indicies for erroneous values.

        The outputs are the following bands:
        mndwi, mbsr, ndvi, awesh, blue, nir, swir1, swir2

        All values should be real valued and non-null.
        """
        stacdl = STACDataLoader(**test_case)
        results, query_gdf = stacdl.query()
        raster_xr = stacdl.load_data(resolution=30)

        indicies_xr = calculate_indicies(raster_xr)

        for name, band in indicies_xr.groupby("band"):
            assert np.isreal(band).all()
            assert not (band == np.nan).any()
