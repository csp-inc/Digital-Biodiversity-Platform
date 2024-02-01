# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

from unittest import TestCase

import numpy as np
import pandas as pd
import xarray as xr
from datasets.indices import calculate_multispectral_indices
from parameterized import parameterized

# TEST XARRAY
TEST_XR = xr.DataArray(
    np.random.rand(3, 10, 107, 110),
    coords={
        "time": pd.date_range("2014-09-06", periods=3),
        "band": [
            "blue",
            "qa_pixel",
            "green",
            "red",
            "swir16",
            "swir22",
            "lwir",
            "coastal",
            "nir08",
            "lwir11",
        ],
        "y": np.linspace(0, 10, 107),
        "x": np.linspace(0, 10, 110),
    },
    dims=("time", "band", "y", "x"),
    name="testarr",
)

# TEST FORMULAS
TEST_FORMULAS_1 = {"ndvi": "([nir08]-[red]) / ([nir08]+[red])"}
TEST_FORMULAS_2 = {
    "abc": "([swir22]-[red] + [blue]) * [red]**2",
    "def": "[coastal] - [swir16]/[swir22] + 0.12",
}

# TEST REFERENCE
coastal = TEST_XR.sel(band="coastal")
blue = TEST_XR.sel(band="blue")
red = TEST_XR.sel(band="red")
nir08 = TEST_XR.sel(band="nir08")
swir16 = TEST_XR.sel(band="swir16")
swir22 = TEST_XR.sel(band="swir22")

TEST_REFERENCE = {
    "ndvi": ((nir08 - red) / (nir08 + red)).values,
    "abc": ((swir22 - red + blue) * red**2).values,
    "def": (coastal - swir16 / swir22 + 0.12).values,
}


class TestIndices(TestCase):
    @parameterized.expand(
        [
            (TEST_XR, TEST_FORMULAS_1),
            (TEST_XR, TEST_FORMULAS_2),
        ],
    )
    def test_calculate_multispectral_indices(self, xarr, formulas):
        """
        Tests calculate_multispectral_indices function,
        """

        xarr_out = calculate_multispectral_indices(xarr, formulas)

        # correct number of indices
        assert len(xarr_out.coords["band"]) == len(formulas.keys())

        # correct values
        for name in list(xarr_out.coords["band"].values):
            assert np.sum(xarr_out.sel(band=name).values - TEST_REFERENCE[name]) == 0.0
