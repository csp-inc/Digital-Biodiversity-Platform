# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import os
import tempfile
from typing import List, Tuple
from unittest import TestCase

import numpy as np
import pandas as pd
import pytest
import xarray as xr
from chips_utils import (
    create_bbox_from_gbif_points,
    create_chip_bbox,
    reflectance_xarr_to_zarr,
)
from parameterized import parameterized
from pytest import approx

TEST_XR_1 = xr.DataArray(
    np.random.rand(3, 100, 100),
    coords={"time": pd.date_range("2014-09-06", periods=3)},
    dims=("time", "y", "x"),
    name="testarr",
)

TEST_XR_2 = xr.DataArray(
    np.random.rand(10, 10, 10),
    coords={"time": pd.date_range("2014-09-06", periods=10)},
    dims=("time", "y", "x"),
    name="testarr",
)

TEST_XR_3 = xr.DataArray(
    np.random.rand(12, 9, 150, 150),
    coords={"time": pd.date_range("2014-09-06", periods=12), "band": np.arange(9)},
    dims=("time", "band", "y", "x"),
    attrs={"spec": 123},
    name="testarr",
)


TEST_GBIF_DF = pd.DataFrame(
    {
        "decimallongitude": np.linspace(10.1, 15.2, 100),
        "decimallatitude": np.linspace(-30, 41.24, 100),
    }
)


class TestChipsUtils(TestCase):
    # @parameterized.expand([(TEST_XR_1,), (TEST_XR_2,)])
    # def test_xarr_to_zarr(self, xarr: xr.DataArray):
    #     """
    #     Tests `chips_utils.xarr_to_zarr` function

    #     Parameters
    #     ----------
    #     xarr : xr.DataArray
    #         Input xarray
    #     """
    #     with tempfile.TemporaryDirectory() as tmpdir:
    #         file_name = os.path.join(tmpdir, "test.zarr")
    #         xarr_to_zarr(xarr, file_name, "w")
    #         assert os.path.exists(file_name)
    #         with pytest.raises(Exception):
    #             xarr_to_zarr(xarr, file_name, "w-")

    @parameterized.expand([(TEST_XR_3,)])
    def test_reflectance_xarr_to_zarr(self, xarr: xr.DataArray):
        """
        Tests `chips_utils.reflectance_xarr_to_zarr` function

        Parameters
        ----------
        xarr : xr.DataArray
            Input xarray
        """

        with tempfile.TemporaryDirectory() as tmpdir:
            file_name = os.path.join(tmpdir, "test.zarr")
            reflectance_xarr_to_zarr(xarr, file_name, "w")
            assert os.path.exists(file_name)
            with pytest.raises(Exception):
                reflectance_xarr_to_zarr(xarr, file_name, "w-")

    @parameterized.expand(
        [
            ([86.9250, 27.9881], 0, [86.9250, 27.9881, 86.9250, 27.9881]),
            ([-122.332, 47.6062], 100, [-122.33, 47.60, -122.33, 47.607]),
        ],
    )
    def test_create_chip_bbox_wgs84(
        self, point: Tuple[float], buffer: float, expected_bbox: List[float]
    ):
        """
        Tests `chips_utils.create_chip_bbox` function

        Parameters
        ----------
        point : Tuple[float]
            Coordinates of chip center point (lon, lat).
        buffer : float
            Buffer size in meters (UTM).
        expected_bbox : List[float]
            Expected result of the function
        """
        x1, y1, x2, y2 = create_chip_bbox(point, buffer, return_wgs84=True)
        assert x1 == approx(expected_bbox[0], 0.01)
        assert y1 == approx(expected_bbox[1], 0.01)
        assert x2 == approx(expected_bbox[2], 0.01)
        assert y2 == approx(expected_bbox[3], 0.01)

    @parameterized.expand(
        [
            ([86.9250, 27.9881], 0, [492625, 3095886.413, 492625, 3095886.413]),
            ([-122.332, 47.6062], 100, [550357.284, 5272060.900, 550364.002, 5272838.863]),
        ],
    )
    def test_create_chip_bbox_utm(
        self, point: Tuple[float], buffer: float, expected_bbox: List[float]
    ):
        """
        Tests `chips_utils.create_chip_bbox` function

        Parameters
        ----------
        point : Tuple[float]
            Coordinates of chip center point (lon, lat).
        buffer : float
            Buffer size in meters (UTM).
        expected_bbox : List[float]
            Expected result of the function
        """
        x1, y1, x2, y2 = create_chip_bbox(point, buffer, return_wgs84=False)
        assert x1 == approx(expected_bbox[0], 0.01)
        assert y1 == approx(expected_bbox[1], 0.01)
        assert x2 == approx(expected_bbox[2], 0.01)
        assert y2 == approx(expected_bbox[3], 0.01)

    @parameterized.expand(
        [
            (TEST_GBIF_DF, [10.1, -30.0, 15.2, 41.24]),
        ],
    )
    def test_create_bbox_from_gbif_points(self, df, expected_bbox: List[float]):
        """
        Tests `chips_utils.create_bbox_from_gbif_points` function

        Parameters
        ----------
        df : pandas DataFrame
            Contains the GBIF data points.
        expected_bbox : List[float]
            Expected result of the function
        """
        x1, y1, x2, y2 = create_bbox_from_gbif_points(df, as_polygon=False)
        assert x1 == approx(expected_bbox[0], 0.01)
        assert y1 == approx(expected_bbox[1], 0.01)
        assert x2 == approx(expected_bbox[2], 0.01)
        assert y2 == approx(expected_bbox[3], 0.01)
