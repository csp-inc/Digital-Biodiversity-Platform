# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import glob
import hashlib
import os
import uuid
import pandas
import pytest

from functools import wraps
from typing import List
from unittest import TestCase
from unittest.mock import MagicMock, patch
from candidate_species_gbif import candidate_species_gbif, main


def patch_all(f):
    """
    Patches all of the library calls that are required to
    make the create_gold_data_table call succeed.
    Most objects are then asserted on in test.
    """

    @patch("candidate_species_gbif.pd")
    @patch("candidate_species_gbif.gbif")
    @patch("pandas.DataFrame.to_csv")
    @wraps(f)
    def functor(*args, **kwargs):
        return f(*args, **kwargs)

    return functor


class TestCandidateSpeciesGbif(TestCase):
    _gbif_vertebrates_file_path = "./"
    _candidate_species_list_path = "./candidate_species.csv"
    _csv_output_directory = "./"
    _correlation_id = uuid.uuid4()

    @patch_all
    def test_candidate_species_reads_gbif_vertebrates_file_path(
        self,
        mock_to_csv: MagicMock,
        mock_gbif: MagicMock,
        mock_pandas: MagicMock,
    ):
        self._initilize_mocks(mock_gbif, mock_pandas)

        self.__call_candidate_species_gbif()

        self.assertTrue(mock_pandas.read_csv.called_with(self._gbif_vertebrates_file_path))

    @patch_all
    def test_candidate_species_subsets_gbif_data_by_minimum_observations(
        self,
        mock_to_csv: MagicMock,
        mock_gbif: MagicMock,
        mock_pandas: MagicMock,
    ):
        self._initilize_mocks(mock_gbif, mock_pandas)

        self.__call_candidate_species_gbif()

        self.assertTrue(mock_gbif.subset_by_minimum_obs.called)

    @patch_all
    def test_candidate_species_calculates_quantile_ranks_to_get_observation_density(
        self,
        mock_to_csv: MagicMock,
        mock_gbif: MagicMock,
        mock_pandas: MagicMock,
    ):
        self._initilize_mocks(mock_gbif, mock_pandas)
        self.__call_candidate_species_gbif()

        self.assertTrue(mock_pandas.qcut.called)

    @patch_all
    def test_candidate_species_calls_get_mean_sample_distance_on_gbif(
        self,
        mock_to_csv: MagicMock,
        mock_gbif: MagicMock,
        mock_pandas: MagicMock,
    ):
        self._initilize_mocks(mock_gbif, mock_pandas)

        self.__call_candidate_species_gbif()

        self.assertTrue(mock_gbif.get_mean_sample_distance.called)

    @patch_all
    def test_candidate_species_writes_3_gold_candidate_data__files_to_csv(
        self,
        mock_to_csv: MagicMock,
        mock_gbif: MagicMock,
        mock_pandas: MagicMock,
    ):
        self._initilize_mocks(mock_gbif, mock_pandas)

        self.__call_candidate_species_gbif()

        # Assert the last file was written
        self._mock_candidate_gold_data.to_csv.assert_called_with(
            self._csv_output_directory + "/" + "c_golddata.csv", index=False
        )

        # Assert that 3 were written.
        self.assertEqual(self._mock_candidate_gold_data.to_csv.call_count, 3)

    @patch_all
    def test_candidate_species_main_enforces_gbif_vertebrates_file_path_is_required(
        self,
        mock_to_csv: MagicMock,
        mock_gbif: MagicMock,
        mock_pandas: MagicMock,
    ):
        self.__initialize_good_args()
        self._args.remove(
            f"--gbif_vertebrates_file_path={self._gbif_vertebrates_file_path}"
        )

        with self.assertRaises(SystemExit) as system_exit_exception:
            main(self._args)
        self.assertEqual(system_exit_exception.exception.code, 2)

    @patch_all
    def test_candidate_species_main_enforces_csv_output_directory_is_required(
        self,
        mock_to_csv: MagicMock,
        mock_gbif: MagicMock,
        mock_pandas: MagicMock,
    ):
        self.__initialize_good_args()
        self._args.remove(f"--csv_output_directory={self._csv_output_directory}")

        with self.assertRaises(SystemExit) as system_exit_exception:
            main(self._args)
        self.assertEqual(system_exit_exception.exception.code, 2)

    @patch_all
    def test_candidate_species_main_enforces_candidate_species_list_path_is_required(
        self,
        mock_to_csv: MagicMock,
        mock_gbif: MagicMock,
        mock_pandas: MagicMock,
    ):
        self.__initialize_good_args()
        self._args.remove(
            f"--candidate_species_list_path={self._candidate_species_list_path}"
        )

        with self.assertRaises(SystemExit) as system_exit_exception:
            main(self._args)
        self.assertEqual(system_exit_exception.exception.code, 2)

    def _initilize_mocks(self, mock_gbif, mock_pandas):
        # Too hard to mock the dataframe, so use a real one, albeit
        # with garbage data.
        data_frame = pandas.DataFrame.from_dict(
            {
                "species": ["a", "b", "c"],
                "obs": ["a", "b", "c"],
                "quantilerank": ["a", "b", "c"],
            }
        )

        mock_gbif.subset_by_minimum_obs.return_value = data_frame

        mock_pandas.qcut.return_value = ["a", "b", "c"]

        self._mock_candidate_gold_data = MagicMock()
        self._mock_reset_index = MagicMock()

        # Set up the concat().reset_index() methods to return the
        # mock_candidate_gold_data.
        mock_pandas.concat.return_value = self._mock_reset_index
        self._mock_reset_index.reset_index.return_value = self._mock_candidate_gold_data

    @pytest.mark.integration
    def test_candidate_species_gbif_create_the_correct_files(self):
        # Create a directory for the test results, this is ignore by git.
        output_dir = "./test_results/candidate_species_gbif/"

        if os.path.exists("./test_results") is False:
            os.mkdir("./test_results")

        if os.path.exists(output_dir) is False:
            os.mkdir(output_dir)
        else:
            # If the directory exists, make sure it is empty.
            for file in os.listdir(output_dir):
                os.remove(output_dir + file)

        # Run the function with known test data.
        candidate_species_gbif(
            candidate_species_list_path="products/biodiversity/dataprep/tests/test_data"
            + "/candidate_species_gbif/candidate_species_list_500k.csv",
            correlation_id=uuid.uuid4(),
            csv_output_directory="test_results/candidate_species_gbif",
            gbif_vertebrates_file_path="products/biodiversity/dataprep/tests/test_data"
            + "/candidate_species_gbif/gbif_vertebrates_subsampled_60m_250k.csv",
            minimum_number_of_observations=500,
            fraction_of_data_for_pairwise_distance=0.5,
            generate=True,
        )

        # Get a list of the expected results files.
        expected_results_directory = (
            os.getcwd()
            + "/products/biodiversity/dataprep/tests/test_data/candidate_species_gbif/"
        )
        expected_results = glob.glob(f"{expected_results_directory}*_golddata.csv")

        # Do we have the right number of files?
        test_result_files = os.listdir(output_dir)
        self.assertEqual(len(test_result_files), len(expected_results))

        # Check each result file matches the expected file using a hash.
        for file in test_result_files:
            with open(output_dir + file, "r") as result:
                result_hash = hashlib.md5(result.read().encode("utf-8"))
                with open(expected_results_directory + file, "r") as expected:
                    expected_hash = hashlib.md5(expected.read().encode("utf-8"))
                    self.assertEqual(expected_hash.hexdigest(), result_hash.hexdigest())

    def __initialize_good_args(self):
        self._args: List[str] = list()
        self._args.append(
            f"--gbif_vertebrates_file_path={self._gbif_vertebrates_file_path}"
        )
        self._args.append(
            f"--candidate_species_list_path={self._candidate_species_list_path}"
        )
        self._args.append(f"--csv_output_directory={self._csv_output_directory}")
        self._args.append("--generate=True")

    def __call_candidate_species_gbif(self):
        candidate_species_gbif(
            candidate_species_list_path=self._candidate_species_list_path,
            correlation_id=self._correlation_id,
            csv_output_directory=self._csv_output_directory,
            gbif_vertebrates_file_path=self._gbif_vertebrates_file_path,
            minimum_number_of_observations=500,
            fraction_of_data_for_pairwise_distance=0.5,
            generate=True,
        )
