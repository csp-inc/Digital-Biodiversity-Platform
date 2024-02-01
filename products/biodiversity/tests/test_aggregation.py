# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

from datetime import datetime
from unittest import TestCase

import numpy as np
import xarray as xr
from aggregation import _aggregate_chip_spatially_perc_total, create_features_from_chip
from parameterized import parameterized


def create_agg_test_case1():
    """Test case of full dataset."""

    data = np.random.normal(1, 2, size=(24, 5, 120, 110)).astype(np.float32)
    # constructing time dimension in a homogeneous way to facilitate testing
    time_coords = sorted(
        [datetime(2020, m, 1) for m in np.arange(1, 13, 1)]
        + [datetime(2020, m, 15) for m in np.arange(1, 13, 1)]
    )

    da = xr.DataArray(
        data=data,
        coords={
            "time": time_coords,
            "band": [f"b{i:02d}" for i in np.arange(data.shape[1])],
            "y": np.arange(data.shape[2]),
            "x": np.arange(data.shape[3]),
        },
        dims=["time", "band", "y", "x"],
    )
    da = da.astype(np.float32)

    tests = []
    settings = {}
    settings["input"] = da
    settings["temporal_aggregation"] = "total"
    settings["spatial_agg_func"] = "mean"
    settings["temporal_agg_func"] = "median"
    settings["output"] = np.nanmedian(np.nanmean(data, axis=(2, 3)), axis=0)[None, :]
    tests.append(settings)

    settings = {}
    settings["input"] = da
    settings["temporal_aggregation"] = "seasonal"
    settings["spatial_agg_func"] = "median"
    settings["temporal_agg_func"] = "min"
    result = np.nanmedian(data, axis=(2, 3))
    settings["output"] = np.array(
        [np.nanmin(item, axis=0) for item in np.vsplit(result, 4)]
    )
    tests.append(settings)

    settings = {}
    settings["input"] = da
    settings["temporal_aggregation"] = "monthly"
    settings["spatial_agg_func"] = "min"
    settings["temporal_agg_func"] = "max"
    result = np.nanmin(data, axis=(2, 3))
    settings["output"] = np.array(
        [np.nanmax(item, axis=0) for item in np.vsplit(result, 12)]
    )
    tests.append(settings)

    return tests


def create_agg_test_case2():
    """Test case with missing band dimension."""

    data = np.random.normal(1, 2, size=(24, 120, 110)).astype(np.float32)
    # constructing time dimension in a homogeneous way to facilitate testing
    time_coords = sorted(
        [datetime(2020, m, 1) for m in np.arange(1, 13, 1)]
        + [datetime(2020, m, 15) for m in np.arange(1, 13, 1)]
    )

    da = xr.DataArray(
        data=data,
        coords={
            "time": time_coords,
            "y": np.arange(data.shape[1]),
            "x": np.arange(data.shape[2]),
        },
        dims=["time", "y", "x"],
    )
    da = da.astype(np.float32)

    tests = []
    settings = {}
    settings["input"] = da
    settings["temporal_aggregation"] = "total"
    settings["spatial_agg_func"] = "mean"
    settings["temporal_agg_func"] = "median"
    settings["output"] = np.array([np.nanmedian(np.nanmean(data, axis=(1, 2)), axis=0)])[
        :, None
    ]
    tests.append(settings)

    settings = {}
    settings["input"] = da
    settings["temporal_aggregation"] = "seasonal"
    settings["spatial_agg_func"] = "median"
    settings["temporal_agg_func"] = "min"
    result = np.nanmedian(data, axis=(1, 2))
    settings["output"] = np.array(
        [np.nanmin(item, axis=0) for item in np.vsplit(result[:, None], 4)]
    )
    tests.append(settings)

    settings = {}
    settings["input"] = da
    settings["temporal_aggregation"] = "monthly"
    settings["spatial_agg_func"] = "min"
    settings["temporal_agg_func"] = "max"
    result = np.nanmin(data, axis=(1, 2))
    settings["output"] = np.array(
        [np.nanmax(item, axis=0) for item in np.vsplit(result[:, None], 12)]
    )
    tests.append(settings)

    return tests


def create_agg_test_case3():
    """Test case with missing time dimension."""

    data = np.random.normal(1, 2, size=(3, 120, 110)).astype(np.float32)

    da = xr.DataArray(
        data=data,
        coords={
            "band": [f"b{i:02d}" for i in np.arange(data.shape[0])],
            "y": np.arange(data.shape[1]),
            "x": np.arange(data.shape[2]),
        },
        dims=["band", "y", "x"],
    )
    da = da.astype(np.float32)

    tests = []
    settings = {}
    settings["input"] = da
    settings["temporal_aggregation"] = "total"
    settings["spatial_agg_func"] = "mean"
    settings["temporal_agg_func"] = "median"
    settings["output"] = np.nanmean(data, axis=(1, 2))[None, :]
    tests.append(settings)

    settings = {}
    settings["input"] = da
    settings["temporal_aggregation"] = "seasonal"
    settings["spatial_agg_func"] = "median"
    settings["temporal_agg_func"] = "min"
    settings["output"] = np.nanmedian(data, axis=(1, 2))[None, :]
    tests.append(settings)

    settings = {}
    settings["input"] = da
    settings["temporal_aggregation"] = "monthly"
    settings["spatial_agg_func"] = "min"
    settings["temporal_agg_func"] = "max"
    settings["output"] = np.nanmin(data, axis=(1, 2))[None, :]
    tests.append(settings)

    return tests


def create_agg_test_case4():
    """Test case with time dimension not covering entire year."""

    data = np.random.normal(1, 2, size=(16, 5, 120, 110)).astype(np.float32)
    # constructing time dimension in a homogeneous way to facilitate testing
    time_coords = sorted(
        [datetime(2020, m, 1) for m in np.arange(1, 9, 1)]
        + [datetime(2020, m, 15) for m in np.arange(1, 9, 1)]
    )

    da = xr.DataArray(
        data=data,
        coords={
            "time": time_coords,
            "band": [f"b{i:02d}" for i in np.arange(data.shape[1])],
            "y": np.arange(data.shape[2]),
            "x": np.arange(data.shape[3]),
        },
        dims=["time", "band", "y", "x"],
    )
    da = da.astype(np.float32)

    tests = []
    settings = {}
    settings["input"] = da
    settings["temporal_aggregation"] = "total"
    settings["spatial_agg_func"] = "mean"
    settings["temporal_agg_func"] = "median"
    settings["output"] = np.nanmedian(np.nanmean(data, axis=(2, 3)), axis=0)[None, :]
    tests.append(settings)

    settings = {}
    settings["input"] = da
    settings["temporal_aggregation"] = "seasonal"
    settings["spatial_agg_func"] = "median"
    settings["temporal_agg_func"] = "min"
    result = np.nanmedian(data, axis=(2, 3))
    settings["output"] = np.array(
        [
            np.nanmin(result[0:6, ...], axis=0),
            np.nanmin(result[6:12, ...], axis=0),
            np.nanmin(result[12:18, ...], axis=0),
            np.array([np.nan] * result.shape[-1]),
        ]
    )
    tests.append(settings)

    settings = {}
    settings["input"] = da
    settings["temporal_aggregation"] = "monthly"
    settings["spatial_agg_func"] = "min"
    settings["temporal_agg_func"] = "max"
    result = np.nanmin(data, axis=(2, 3))
    settings["output"] = np.array(
        [np.nanmax(item, axis=0) for item in np.vsplit(result, 8)]
        + [np.array([np.nan] * result.shape[-1])] * 4
    )
    tests.append(settings)

    return tests


def create_agg_test_case5():
    """
    Creates test for categorical spatial aggregation.
    Basic test case.  shape = (2, 1, 4, 4). Default threshold
    """

    arr = [
        [[[0, 1, 0, 1], [1, 2, 2, 0], [0, 0, 2, 1], [2, 2, 2, 2]]],
        [[[1, 1, 1, 1], [1, 2, 2, 0], [0, 0, 2, 2], [2, 2, 2, 2]]],
    ]
    kwargs = {"num_bins": 2, "category_dict": {0: 0, 1: 0, 2: 1}}
    expected_out = np.array([[0.5625, 0.4375], [0.5, 0.5]])
    return {"data": np.array(arr), "kwargs": kwargs, "expected_out": expected_out}


def create_agg_test_case6():
    """
    Creates test for categorical spatial aggregation.
    Input has some missing data.
    """

    arr = [
        [[[np.nan, 1, 0, 1], [1, 2, 2, 0], [0, 0, 2, 1], [2, np.nan, 2, 2]]],
        [[[1, 1, 1, 1], [np.nan, 2, 2, 0], [0, 0, 2, 2], [2, 2, np.nan, 2]]],
    ]
    threshold = 1
    kwargs = {
        "num_bins": 2,
        "category_dict": {0: 0, 1: 0, 2: 1},
        "threshold": threshold,
    }
    expected_out = np.array([[0.5, 0.375], [0.4375, 0.4375]])
    return {"data": np.array(arr), "kwargs": kwargs, "expected_out": expected_out}


def create_agg_test_case7():
    """
    Creates test for categorical spatial aggregation.
    Input is entirely missing data.
    """

    arr = np.ones(shape=(2, 1, 4, 4)) * np.nan
    threshold = 1
    kwargs = {
        "num_bins": 2,
        "category_dict": {0: 0, 1: 0, 2: 1},
        "threshold": threshold,
    }
    expected_out = np.array([[np.nan, np.nan], [np.nan, np.nan]])
    return {"data": arr, "kwargs": kwargs, "expected_out": expected_out}


def create_agg_test_case8():
    """
    Creates test for categorical spatial aggregation.
    Input is some missing data, but threshold is high enough to remove first time scene
    """

    arr = [
        [
            [
                [np.nan, 1, 0, 1],
                [np.nan, np.nan, 2, np.nan],
                [0, np.nan, 2, np.nan],
                [2, np.nan, 2, 2],
            ]
        ],
        [[[1, 1, 1, 1], [np.nan, 2, 2, 0], [0, 0, 2, 2], [2, 2, np.nan, 2]]],
    ]
    threshold = 0.25
    kwargs = {
        "num_bins": 2,
        "category_dict": {0: 0, 1: 0, 2: 1},
        "threshold": threshold,
    }
    expected_out = np.array([[np.nan, np.nan], [0.4375, 0.4375]])
    return {"data": np.array(arr), "kwargs": kwargs, "expected_out": expected_out}


class TestAggregation(TestCase):
    @parameterized.expand(
        [
            (1, create_agg_test_case1()),
            (2, create_agg_test_case2()),
            (3, create_agg_test_case3()),
            (4, create_agg_test_case4()),
        ],
    )
    def test_create_features_from_chip(self, test_run, test_settings):
        for k, settings in enumerate(test_settings):
            result = create_features_from_chip(
                xarr=settings["input"],
                name="test",
                temporal_aggregation=settings["temporal_aggregation"],
                spatial_agg_func=settings["spatial_agg_func"],
                temporal_agg_func=settings["temporal_agg_func"],
                return_as_dict=False,
            )

            np.testing.assert_allclose(
                result, settings["output"], atol=1e-5, equal_nan=True
            )

    @parameterized.expand(
        [
            (1, create_agg_test_case5()),
            (2, create_agg_test_case6()),
            (3, create_agg_test_case7()),
            (4, create_agg_test_case8()),
        ],
    )
    def test__aggregate_chip_spatially_perc_total(self, test_run, args):
        """Tests spatial aggregation of categorical values by percent total"""
        output = _aggregate_chip_spatially_perc_total(data=args["data"], **args["kwargs"])
        np.testing.assert_equal(output, args["expected_out"])
