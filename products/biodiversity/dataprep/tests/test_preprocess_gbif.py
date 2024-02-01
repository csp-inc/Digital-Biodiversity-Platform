# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import unittest
import uuid
from functools import wraps
from typing import List
from unittest.mock import MagicMock, patch

from preprocess_gbif import main, preprocess_gbif


def patch_all(f):
    """
    Patches all of the library calls that are required to
    make the read_gbif call succeed. Most objects are then
    asserted on in test.
    """

    @patch("preprocess_gbif.dask.initialize_dask_cluster")
    @patch("preprocess_gbif.gbif.read_gbif")
    @patch("builtins.open")
    @patch("preprocess_gbif.gpd.read_file")
    @patch("preprocess_gbif.gbif.gbif_spatial_filter")
    @patch("preprocess_gbif.dask_gpd.GeoDataFrame")
    @patch("preprocess_gbif.gbif.gbif_subsample")
    @wraps(f)
    def functor(*args, **kwargs):
        return f(*args, **kwargs)

    return functor


class TestPreprocessGbif(unittest.TestCase):
    _valid_ecoregions_path = "./data/file.zip"
    _valid_output_file_path = "./results/file.zip"
    _valid_parameters = '{"key": "US_L3NAME", "values": "[\\"Puget Lowland\\", \\"North Cascades\\"]", "stateprovince": "Washington"}'  # noqa: E501
    _valid_spatial_filter_projection_code = "epsg:12345"
    _valid_correlation_id = uuid.uuid4()

    @patch_all
    def test_process_gbif_reads_gbif_data(
        self,
        gbif_subsample: MagicMock,
        geo_data_frame: MagicMock,
        gbif_spatial_filter: MagicMock,
        read_file: MagicMock,
        open: MagicMock,
        read_gbif: MagicMock,
        mock_initialize_dask_cluster: MagicMock,
    ):
        preprocess_gbif(
            self._valid_ecoregions_path,
            self._valid_output_file_path,
            self._valid_parameters,
            self._valid_spatial_filter_projection_code,
            self._valid_correlation_id,
        )

        self.assertTrue(read_gbif.called)
        self.assertEqual(read_gbif.call_args.kwargs["npartitions"], 256)
        self.assertEqual(
            read_gbif.call_args.kwargs["correlation_id"], self._valid_correlation_id
        )
        self.assertFalse(read_gbif.call_args.kwargs["first_partition_only"])

    @patch_all
    def test_process_gbif_reads_input_file_path(
        self,
        gbif_subsample: MagicMock,
        geo_data_frame: MagicMock,
        gbif_spatial_filter: MagicMock,
        read_file: MagicMock,
        open: MagicMock,
        read_gbif: MagicMock,
        mock_initialize_dask_cluster: MagicMock,
    ):
        preprocess_gbif(
            self._valid_ecoregions_path,
            self._valid_output_file_path,
            self._valid_parameters,
            self._valid_spatial_filter_projection_code,
            self._valid_correlation_id,
        )

        self.assertTrue(self._valid_ecoregions_path in open.call_args.args)
        self.assertTrue("rb" in open.call_args.args)

    @patch_all
    def test_process_gbif_reads_file_to_memory(
        self,
        gbif_subsample: MagicMock,
        geo_data_frame: MagicMock,
        gbif_spatial_filter: MagicMock,
        read_file: MagicMock,
        open: MagicMock,
        read_gbif: MagicMock,
        mock_initialize_dask_cluster: MagicMock,
    ):
        preprocess_gbif(
            self._valid_ecoregions_path,
            self._valid_output_file_path,
            self._valid_parameters,
            self._valid_spatial_filter_projection_code,
            self._valid_correlation_id,
        )

        self.assertTrue(read_file.called)

    @patch_all
    def test_process_gbif_applies_spatial_filter(
        self,
        gbif_subsample: MagicMock,
        geo_data_frame: MagicMock,
        gbif_spatial_filter: MagicMock,
        read_file: MagicMock,
        open: MagicMock,
        read_gbif: MagicMock,
        mock_initialize_dask_cluster: MagicMock,
    ):
        preprocess_gbif(
            self._valid_ecoregions_path,
            self._valid_output_file_path,
            self._valid_parameters,
            self._valid_spatial_filter_projection_code,
            self._valid_correlation_id,
        )

        self.assertTrue(gbif_spatial_filter.called)

    @patch_all
    def test_process_gbif_subsamples_the_data(
        self,
        gbif_subsample: MagicMock,
        geo_data_frame: MagicMock,
        gbif_spatial_filter: MagicMock,
        read_file: MagicMock,
        open: MagicMock,
        read_gbif: MagicMock,
        mock_initialize_dask_cluster: MagicMock,
    ):
        preprocess_gbif(
            self._valid_ecoregions_path,
            self._valid_output_file_path,
            self._valid_parameters,
            self._valid_spatial_filter_projection_code,
            self._valid_correlation_id,
        )

        self.assertTrue(gbif_subsample.called)

    def test_preprocess_gbif_enforces_ecoregions_path_as_required(self):
        self.__initialize_good_args()
        self._args.remove(f"--ecoregions_file_path={self._valid_ecoregions_path}")

        with self.assertRaises(SystemExit) as system_exit_exception:
            main(self._args)
        self.assertEqual(system_exit_exception.exception.code, 2)

    def test_preprocess_gbif_enforces_output_path_as_required(self):
        self.__initialize_good_args()
        self._args.remove(f"--output_file_path={self._valid_output_file_path}")

        with self.assertRaises(SystemExit) as system_exit_exception:
            main(self._args)
        self.assertEqual(system_exit_exception.exception.code, 2)

    def test_preprocess_gbif_enforces_parameters_as_required(self):
        self.__initialize_good_args()
        self._args.remove(f"--parameters_string={self._valid_parameters}")

        with self.assertRaises(SystemExit) as system_exit_exception:
            main(self._args)
        self.assertEqual(system_exit_exception.exception.code, 2)

    def test_preprocess_gbif_enforces_spatial_filter_projection_code_as_required(
        self,
    ):
        self.__initialize_good_args()
        self._args.remove(
            f"--spatial_filter_projection_code={self._valid_spatial_filter_projection_code}"
        )

        with self.assertRaises(SystemExit) as system_exit_exception:
            main(self._args)
        self.assertEqual(system_exit_exception.exception.code, 2)

    @patch_all
    def test_preprocess_gbif_main_initializes_dask_cluster(
        self,
        gbif_subsample: MagicMock,
        geo_data_frame: MagicMock,
        gbif_spatial_filter: MagicMock,
        read_file: MagicMock,
        open: MagicMock,
        read_gbif: MagicMock,
        mock_initialize_dask_cluster: MagicMock,
    ):
        self.__initialize_good_args()
        main(self._args)
        assert mock_initialize_dask_cluster.called

    def __initialize_good_args(self):
        self._args: List[str] = list()
        self._args.append(f"--ecoregions_file_path={self._valid_ecoregions_path}")
        self._args.append(f"--output_file_path={self._valid_output_file_path}")
        self._args.append(f"--parameters_string={self._valid_parameters}")
        self._args.append(
            f"--spatial_filter_projection_code={self._valid_spatial_filter_projection_code}"
        )
