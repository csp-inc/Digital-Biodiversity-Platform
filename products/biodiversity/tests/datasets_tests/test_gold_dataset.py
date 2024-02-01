# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

from unittest import TestCase

import numpy as np
import pandas as pd
import pytest
from datasets.gold_dataset import combine_data_tables
from parameterized import parameterized

TEST_DF1 = pd.DataFrame()
TEST_DF1["gbifid"] = np.arange(500000)
for i in range(32):
    TEST_DF1[f"col{i}"] = np.random.random(500000)

TEST_DF2 = pd.DataFrame()
TEST_DF2["gbifid"] = list(np.arange(497000)) + list(np.arange(10000000, 10003000, 1))
for i in range(7):
    TEST_DF2[f"col20{i}"] = np.random.random(500000)
TEST_DF2["col201"][3000:4200] = np.nan


class TestGoldDataset(TestCase):
    @parameterized.expand(
        [
            (
                1,
                [TEST_DF1, TEST_DF2],
                {"handle_nans": "drop", "fill_value": 0, "rename_cols": None},
            ),
            (
                2,
                [TEST_DF1, TEST_DF2],
                {"handle_nans": "fill", "fill_value": -5, "rename_cols": None},
            ),
            (
                3,
                [TEST_DF1, TEST_DF2],
                {"handle_nans": "fill", "fill_value": "mean", "rename_cols": None},
            ),
            (
                4,
                [TEST_DF1, TEST_DF2],
                {
                    "handle_nans": "drop",
                    "fill_value": 0,
                    "rename_cols": {"col0": "rename_col_test"},
                },
            ),
        ],
    )
    def test_combine_data_tables(self, test_run, dfs, test_settings):
        dfout = combine_data_tables(
            dfs,
            handle_nans=test_settings["handle_nans"],
            fill_value=test_settings["fill_value"],
            rename_cols=test_settings["rename_cols"],
        )

        if test_run == 1:
            assert len(dfout) == 495800
            assert len(dfout.columns) == 39
            assert np.sum(dfout.isna().values) == 0
            assert dfout["col201"].mean() == pytest.approx(0.5, abs=1e-3)
        elif test_run == 2:
            assert len(dfout) == 497000
            assert len(dfout.columns) == 39
            assert np.sum(dfout.isna().values) == 0
            assert dfout["col201"].mean() < 0.5
        elif test_run == 3:
            assert len(dfout) == 497000
            assert len(dfout.columns) == 39
            assert np.sum(dfout.isna().values) == 0
            assert dfout["col201"].mean() == pytest.approx(0.5, abs=1e-3)
        elif test_run == 4:
            assert len(dfout) == 495800
            assert len(dfout.columns) == 39
            assert np.sum(dfout.isna().values) == 0
            assert dfout["col201"].mean() == pytest.approx(0.5, abs=1e-3)
            assert "rename_col_test" in dfout.columns
