# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import pickle
from unittest import TestCase
from unittest.mock import DEFAULT, MagicMock, patch

import numpy as np
import pandas as pd
from onthefly_proc import (
    dask_nasadem_features_wrapper_onthefly,
    dask_water_features_wrapper_onthefly,
)

TEST_DF1 = pd.DataFrame()
TEST_DF1["gbifid"] = np.arange(1000)
TEST_DF1["decimallongitude"] = -122.061066
TEST_DF1["decimallatitude"] = 47.88422
for i in range(30):
    TEST_DF1[f"col{i}"] = np.random.random(1000)

TEST_DF2 = pd.DataFrame()
TEST_DF2["gbifid"] = np.arange(10)
TEST_DF2["decimallongitude"] = -122.061066
TEST_DF2["decimallatitude"] = 47.88422
for i in range(30):
    TEST_DF2[f"col{i}"] = np.random.random(10)

with open("products/biodiversity/tests/test_data/water_xarr.pickle", "rb") as handle:
    water_xarr = pickle.load(handle)


class TestOnTheFlyProc(TestCase):
    @patch("onthefly_proc.create_chip_bbox")
    @patch("numpy.min")
    @patch("onthefly_proc.calculate_features")
    def test_dask_nasadem_features_wrapper_onthefly_no_exception_returns_rows(
        self,
        calculate_features,
        numpy_min,
        create_chip_bbox,
    ):
        numpy_min.return_value = 10
        calculate_features.return_value = {"foo": "bar"}
        result = dask_nasadem_features_wrapper_onthefly(TEST_DF1, MagicMock(), 500)
        self.assertTrue(create_chip_bbox.called)
        self.assertTrue(calculate_features.called)
        self.assertEqual(1000, len(result))

    @patch("onthefly_proc.create_chip_bbox")
    @patch("numpy.min")
    @patch("onthefly_proc.calculate_features")
    def test_dask_nasadem_features_wrapper_onthefly_exception_returns_rows(
        self,
        calculate_features,
        numpy_min,
        create_chip_bbox,
    ):
        numpy_min.return_value = 10
        calculate_features.return_value = {"foo": "bar"}

        # Generate a side_effect list with 1 exception every 100 calls
        mock_calculate_features_with_exception = []
        for i in range(10):
            for j in range(99):
                # Default return_value
                mock_calculate_features_with_exception.append(DEFAULT)
            # Exception (1 in 100)
            mock_calculate_features_with_exception.append(Exception("fail"))

        calculate_features.side_effect = mock_calculate_features_with_exception

        result = dask_nasadem_features_wrapper_onthefly(TEST_DF1, MagicMock(), 500)

        self.assertTrue(create_chip_bbox.called)
        self.assertTrue(calculate_features.called)
        self.assertEqual(990, len(result))

    @patch("numpy.min")
    @patch("onthefly_proc.interpret_dswe_from_tests")
    @patch("onthefly_proc.create_features_from_chip")
    def test_dask_water_features_wrapper_onthefly_no_exception_returns_rows(
        self,
        create_features_from_chip,
        interpret_dswe_from_tests,
        numpy_min,
    ):
        numpy_min.return_value = 10
        create_features_from_chip.return_value = {"foo": "bar"}
        result = dask_water_features_wrapper_onthefly(TEST_DF2, water_xarr, 500)
        self.assertTrue(create_features_from_chip.called)
        self.assertEqual(10, len(result))

    @patch("numpy.min")
    @patch("onthefly_proc.interpret_dswe_from_tests")
    @patch("onthefly_proc.create_features_from_chip")
    def test_dask_water_features_wrapper_onthefly_exception_returns_rows(
        self,
        create_features_from_chip,
        interpret_dswe_from_tests,
        numpy_min,
    ):
        numpy_min.return_value = 10
        create_features_from_chip.return_value = {"foo": "bar"}

        # Generate a side_effect list with 1 exception every 10 calls
        mock_create_features_from_chip_with_exception = []
        for j in range(9):
            # Default return_value
            mock_create_features_from_chip_with_exception.append(DEFAULT)
        # Exception (1 in 100)
        mock_create_features_from_chip_with_exception.append(Exception("fail"))

        create_features_from_chip.side_effect = (
            mock_create_features_from_chip_with_exception
        )

        result = dask_water_features_wrapper_onthefly(TEST_DF2, water_xarr, 500)
        self.assertTrue(create_features_from_chip.called)
        self.assertEqual(9, len(result))
