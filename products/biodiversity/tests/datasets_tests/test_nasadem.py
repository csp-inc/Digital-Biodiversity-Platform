# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

from unittest import TestCase

import numpy as np
import pandas as pd
import pytest
import xarray as xr
from datasets.nasadem import calculate_aspect, calculate_features, center_crop, preprocess
from parameterized import parameterized

TEST_DS_1 = xr.Dataset(
    data_vars={"data": (["band", "time", "x", "y"], np.ones((1, 1, 3, 3)))},
    coords={
        "time": pd.date_range("2014-09-06", periods=1),
        "lat": [89.979167, 89.9375, 89.895833],
        "lon": [-179.979167, -179.9375, -179.895833],
        "band": ["elevation"],
    },
)

TEST_DS_2 = xr.Dataset(
    data_vars={"data": (["band", "time", "x", "y"], np.ones((1, 1, 3, 3)))},
    coords={
        "time": pd.date_range("2014-09-06", periods=1),
        "lat": [89.979167, 89.9375, 89.895833],
        "lon": [-179.979167, -179.9375, -179.895833],
        "band": ["elevation"],
    },
)


class TestNasadem(TestCase):
    @parameterized.expand(
        [
            (
                xr.DataArray(
                    np.random.rand(3, 2, 2),
                    coords={
                        "time": pd.date_range("2014-09-06", periods=3),
                        "lat": [89.979167, 89.9375],
                        "lon": [-179.979167, -179.9375],
                    },
                ),
            ),
            (
                xr.DataArray(
                    np.random.rand(1, 2, 2, 1),
                    coords={
                        "time": pd.date_range("2014-09-06", periods=1),
                        "lat": [89.979167, 89.9375],
                        "lon": [-179.979167, -179.9375],
                        "band": ["band1"],
                    },
                ),
            ),
            (xr.DataArray(np.random.rand(1, 2, 3)),),
        ],
    )
    def test_preprocess(self, xarr: xr.DataArray):
        """
        Tests `preprocess` function from `datasets.nasadem` module

        Parameters
        ----------
        xarr : xr.DataArray
            Input elevation array to test the function
        """
        prep_xr = preprocess(xarr)
        assert 1 not in prep_xr.shape
        assert len(prep_xr.shape) == 2

    @parameterized.expand(
        [
            (xr.DataArray([[0, 0], [0, 0]]), None, 0.0),
            (xr.DataArray([[1, 1, 1], [1, 1, 1], [1, 1, 1]]), None, -1.0),
            (xr.DataArray([[1, 1, 1], [1, 1, 1], [0, 0, 0]]), None, 180.0),
            (xr.DataArray([[1, 1, 0], [1, 1, 0], [1, 1, 0]]), None, 90.0),
            (xr.DataArray([[1, 1, 1], [1, 1, 1], [1, 1, 1]]), "sin", -1.0),
            (xr.DataArray([[1, 1, 1], [1, 1, 1], [0, 0, 0]]), "sin", 1),
            (xr.DataArray([[1, 1, 0], [1, 1, 0], [1, 1, 0]]), "sin", 2),
            (xr.DataArray([[1, 1, 1], [1, 1, 1], [1, 1, 1]]), "cos", -1.0),
            (xr.DataArray([[1, 1, 1], [1, 1, 1], [0, 0, 0]]), "cos", 0),
            (xr.DataArray([[1, 1, 0], [1, 1, 0], [1, 1, 0]]), "cos", 1),
        ],
    )
    def test_calculate_aspect(
        self, xarr: xr.DataArray, transform: str, expected_aspect_sum: float
    ):
        """
        Tests `calculate_aspect` function from `datasets.nasadem` module

        Parameters
        ----------
        xarr : xr.DataArray
            Input elevation array to test the function
        transform : str, optional
            Transformation to be applied to the aspect array
        expected_aspect_sum : float
            Expected sum of the resulting aspect array
        """
        aspect_sum = calculate_aspect(xarr, transform).sum(skipna=True)
        assert aspect_sum == pytest.approx(expected_aspect_sum, 0.01)

    @parameterized.expand(
        [
            (
                np.array([[1, 2, 3], [4, 5, 6], [7, 8, 9], [10, 11, 12], [13, 14, 15]]),
                np.array([[4, 5, 6], [7, 8, 9], [10, 11, 12]]),
            ),
            (
                np.array([[1, 2, 3, 4, 5], [4, 5, 6, 7, 8]]),
                np.array([[2, 3, 4], [5, 6, 7]]),
            ),
            (
                np.array([[1, 2, 3], [4, 5, 6], [7, 8, 9]]),
                np.array([[1, 2, 3], [4, 5, 6], [7, 8, 9]]),
            ),
        ],
    )
    def test_center_crop(self, arr: np.ndarray, expected_arr: np.ndarray):
        """
        tests `center_crop` function

        Parameters
        ----------
        arr : np.ndarray
            Input 2D array
        expected_arr : np.ndarray
            Expected resulting 2D array
        """
        result_arr = center_crop(arr)
        assert (result_arr == expected_arr).all()

    @parameterized.expand(
        [(TEST_DS_1, 1.0, -1.0, 0.0), (TEST_DS_2, 0.666, 26.565, 0.0)],
    )
    def test_calculate_features(
        self, ds: xr.Dataset, mean_elevation: float, mean_aspect: float, mean_slope: float
    ):
        """
        Test `calculate_features` function

        Parameters
        ----------
        ds : xr.Dataset
            Input elevation dataset
        mean_elevation : float
            Expected mean elevation
        mean_aspect : float
            Expected mean aspect
        mean_slope : float
            Expected mean slope
        """
        result = calculate_features(ds)
        assert result["nasadem_elevation_total"] == pytest.approx(mean_elevation, 3)
        assert result["nasadem_aspect_total"] == pytest.approx(mean_aspect, 3)
        assert result["nasadem_slope_total"] == pytest.approx(mean_slope, 3)
