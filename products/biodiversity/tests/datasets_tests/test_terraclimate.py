# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

from typing import Tuple
from unittest import TestCase

import numpy as np
import pandas as pd
import pytest
import xarray as xr
from datasets.terraclimate import (
    get_nearest_point,
    preprocess_for_aggregation,
    transform_dataset,
)
from parameterized import parameterized

TEST_XR = xr.DataArray(
    np.random.rand(5, 5, 5),
    coords={
        "time": pd.date_range("2014-09-06", periods=5),
        "lat": [89.979167, 89.9375, 89.895833, 89.854167, 89.8125],
        "lon": [-179.979167, -179.9375, -179.895833, -179.854167, -179.8125],
    },
    dims=("time", "lat", "lon"),
).to_dataset(name="data")

TEST_DS = xr.Dataset(
    data_vars={
        "var1": (["time", "lat", "lon"], np.random.rand(5, 5, 5)),
        "var2": (["time", "lat", "lon"], np.random.rand(5, 5, 5)),
        "var3": (["time", "lat", "lon"], np.random.rand(5, 5, 5)),
    },
    coords={
        "time": pd.date_range("2014-09-06", periods=5),
        "lat": [89.979167, 89.9375, 89.895833, 89.854167, 89.8125],
        "lon": [-179.979167, -179.9375, -179.895833, -179.854167, -179.8125],
    },
)

TEST_DS_2 = xr.Dataset(
    data_vars={"data": (["feature", "time", "lat", "lon"], np.random.rand(5, 5, 5, 5))},
    coords={
        "time": pd.date_range("2014-09-06", periods=5),
        "lat": [89.979167, 89.9375, 89.895833, 89.854167, 89.8125],
        "lon": [-179.979167, -179.9375, -179.895833, -179.854167, -179.8125],
        "feature": ["band1", "band2", "band3", "band4", "band5"],
    },
)

TEST_DS_3 = xr.Dataset(
    data_vars={"data": (["band", "time", "lat", "lon"], np.random.rand(2, 2, 2, 2))},
    coords={
        "time": pd.date_range("2014-09-06", periods=2),
        "lat": [89.979167, 89.9375],
        "lon": [-179.979167, -179.9375],
        "band": ["band1", "band2"],
    },
)


class TestTerraclimate(TestCase):
    @parameterized.expand(
        [
            (TEST_XR, [89.96, -179.96], [89.979, -179.079]),
            (TEST_XR, [89.88, -179.87], [89.854, -179.854]),
            (TEST_XR, [89.80, -179.80], [89.812, -179.812]),
        ],
    )
    def test_get_nearest_point(
        self, xarr: xr.DataArray, point: Tuple[float], expected_point: Tuple[float]
    ):
        """
        Tests `get_nearest_point` function

        Parameters
        ----------
        xarr : xr.DataArray
            Input xarray with points
        point : Tuple[float]
            Point of interest
        expected_point : Tuple[float]
            Expected nearest point
        """
        result = get_nearest_point(xarr, point)
        result_lat = result.coords["lat"].values[0]
        result_lon = result.coords["lon"].values[0]
        assert expected_point[0] == pytest.approx(result_lat, 3)
        assert expected_point[1] == pytest.approx(result_lon, 3)

    @parameterized.expand([(TEST_DS,)])
    def test_transform_dataset(self, ds: xr.Dataset):
        """
        Tests `transform_dataset` function

        Parameters
        ----------
        ds : xr.Dataset
            Input dataset
        """
        result_ds = transform_dataset(ds)
        assert result_ds.dims["band"] == len(ds.keys())
        assert result_ds.dims["lat"] == ds.dims["lat"]
        assert result_ds.dims["lon"] == ds.dims["lon"]
        assert result_ds.dims["time"] == ds.dims["time"]
        assert len(result_ds.dims) == len(ds.dims) + 1
        assert len(result_ds.data_vars) == 1
        for i, band in enumerate(ds.data_vars):
            arr1 = ds[band].to_numpy()
            arr2 = result_ds["data"].isel(band=i).to_numpy()
            assert (arr1 == arr2).all()

    @parameterized.expand([(TEST_DS_2, "feature"), (TEST_DS_3, "band")])
    def test_preprocess_for_aggregation(self, ds: xr.Dataset, band_name: str):
        """
        Tests `preprocess_for_aggregation` function

        Parameters
        ----------
        ds : xr.Dataset
            Input dataset
        band_name : str
            Name of the coord containing band
        """
        ds_prep = preprocess_for_aggregation(ds)
        assert list(sorted(ds_prep.indexes)) == ["band", "time", "x", "y"]
        assert list(ds_prep.data.dims) == ["time", "band", "y", "x"]
        assert list(sorted(ds_prep.coords)) == ["band", "time", "x", "y"]
        assert list(ds_prep.coords["band"]) == list(ds.coords[band_name])
