# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import datetime
import os
import unittest
from unittest.mock import MagicMock, patch

from feature_aggregation import (
    collect_feature_settings,
    execute_dask_procs,
    feature_aggregation,
    get_latest_file_number,
    query_stac,
)
from parameterized import parameterized


class TestFeatureAggregation(unittest.TestCase):
    @patch("os.listdir")
    def test_get_latest_file_number_empty_dir_returns_minus_one(self, listdir: MagicMock):
        listdir.return_value = []
        index = get_latest_file_number("/fake/path")
        self.assertTrue(listdir.called)
        self.assertEqual(-1, index)

    @patch("os.listdir")
    def test_get_latest_file_number_index_0_returns_0(self, listdir: MagicMock):
        listdir.return_value = [
            "landsat8_0.csv",
        ]
        index = get_latest_file_number("/fake/path")
        self.assertTrue(listdir.called)
        self.assertEqual(0, index)

    @patch("os.listdir")
    def test_get_latest_file_number_returns_last_index(self, listdir: MagicMock):
        listdir.return_value = [
            "landsat8_68.csv",
            "landsat8_77.csv",
            "landsat8_104.csv",
            "landsat8_138.csv",
        ]
        index = get_latest_file_number("/fake/path")
        self.assertTrue(listdir.called)
        self.assertEqual(138, index)
        pass

    @patch("os.listdir")
    def test_get_latest_file_number_incorrect_list_fails(self, listdir: MagicMock):
        listdir.return_value = [
            "foo.csv",
            "bar.csv",
        ]
        with self.assertRaises(Exception):
            index = get_latest_file_number("/fake/path")
            self.assertTrue(listdir.called)
            self.assertEqual(138, index)

    def test_collect_feature_settings(self):
        tests = [
            {"feature": "landsat8", "collection": "landsat-c2-l2"},
            {"feature": "indices", "collection": "landsat-c2-l2"},
            {"feature": "water", "collection": "landsat-c2-l2"},
            {"feature": "nasadem", "collection": "nasadem"},
            {"feature": "gnatsgo", "collection": "gnatsgo-rasters"},
            {"feature": "terraclimate", "collection": "terraclimate"},
        ]
        for i in tests:
            with self.subTest(i=i):
                parameters = collect_feature_settings(i["feature"])
                self.assertIsNotNone(parameters[0])
                self.assertEqual(i["collection"], parameters[0][0])

    def test_collect_feature_settings_custom(self):
        parameters = collect_feature_settings("foo", custom_settings=True)
        self.assertIsNone(parameters[0])

    @patch("dask.compute")
    @patch("pandas.concat")
    def test_execute_dask_procs(self, pd_concat: MagicMock, dask_compute: MagicMock):
        concat_return = pd_concat.return_value = MagicMock(name="concat_return")
        execute_dask_procs([123], datetime.datetime.now(), 1, 1, save_to_file="foo")
        self.assertTrue(dask_compute.called)
        self.assertTrue(pd_concat.called)
        self.assertTrue(concat_return.to_csv.called)

    @patch("feature_aggregation.STACDataLoader")
    def test_query_stac_successful_return_results(self, stac_loader: MagicMock):
        stac_loader_return = stac_loader.return_value = MagicMock(name="stac_loader_return")
        stac_loader_return.query.return_value = ("foo", "bar")
        results = query_stac(
            MagicMock(),
            MagicMock(),
            MagicMock(),
            MagicMock(),
            MagicMock(),
            MagicMock(),
            MagicMock(),
        )
        self.assertEqual(1, stac_loader.call_count)
        self.assertEqual(3, len(results))

    @patch("feature_aggregation.STACDataLoader")
    def test_query_stac_throws_tries_n_times(self, stac_loader: MagicMock):
        stac_loader_return = stac_loader.return_value = MagicMock(name="stac_loader_return")
        stac_loader_return.query.side_effect = Exception("API Error")
        with self.assertRaises(Exception):
            query_stac(
                MagicMock(),
                MagicMock(),
                MagicMock(),
                MagicMock(),
                MagicMock(),
                MagicMock(),
                MagicMock(),
                retries=3,
                wait_time=1,
            )
            self.assertEqual(3, stac_loader.call_count)

    @parameterized.expand(
        [
            ("landsat8",),
            ("indices",),
            ("water",),
            ("nasadem",),
            ("gnatsgo",),
            ("terraclimate",),
        ]
    )
    @patch("feature_aggregation.execute_dask_procs")
    def test_integration_feature_aggregation(self, feature, execute_dask_procs: MagicMock):
        input_file = os.path.join(
            os.path.dirname(__file__),
            "../..",
            "data",
            "gbif_subsampled_one_partition_10.csv",
        )
        output_folder = os.path.join(
            os.path.dirname(__file__),
            "../..",
            "results",
        )
        feature_aggregation(feature, input_file, output_folder)
        self.assertTrue(execute_dask_procs.called)
