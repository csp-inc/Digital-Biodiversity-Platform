# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import uuid
from functools import wraps
from unittest import TestCase
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
from mlops.prep_src.prep import main, prepare_dataset
from pandas.testing import assert_frame_equal


def patch_all(f):
    """
    Patches all of the library calls that are required to
    make the main function call succeed.
    Most objects are then asserted on in test.
    """

    @patch("mlops.prep_src.prep.os.path.join")
    @patch("mlops.prep_src.prep.read_from_csv")
    @patch("mlops.prep_src.prep.write_to_csv")
    @patch("mlops.prep_src.prep.prepare_dataset")
    @wraps(f)
    def functor(*args, **kwargs):
        return f(*args, **kwargs)

    return functor


class TestPrep(TestCase):
    _raw_data_gbif_path = "./"
    _raw_data_species_path = "./"
    _species_name = "foo"
    _prep_data_path = "./"
    _columns_to_drop = []
    _target_col_name = "col"
    _drop_nans = True
    _correlation_id = uuid.uuid4()

    def test_prepare_dataset(self):
        gbif_columns = ["gbifid", "col1", "col2", "col3"]
        gbif_rows = [
            [1.0, "a", "b", "c"],
            [2.1, "d", "e", "f"],
            [3, "g", "h", "i"],
            [6, None, "h", "i"],
        ]

        species_columns = ["gbifid", "col1", "col5", "target"]
        species_rows = [
            [1, "ai", "bc", "ABSENT"],
            [4.0, "dd", "de", "PRESENT"],
            [2.2, "gk", "hl", "PRESENT"],
            [6, "f", None, "ABSENT"],
        ]

        expected_columns = ["gbifid", "col1_all", "col2", "col3", "col1_species"]
        expected_rows = [[1, "a", "b", "c", "ai"], [2, "d", "e", "f", "gk"]]
        expected_features_df = pd.DataFrame(data=expected_rows, columns=expected_columns)

        expected_targets = np.array([0, 1])

        mock_gbif_df = pd.DataFrame(data=gbif_rows, columns=gbif_columns)
        mock_species_df = pd.DataFrame(data=species_rows, columns=species_columns)

        features, targets = prepare_dataset(
            mock_gbif_df, mock_species_df, True, ["col5"], "target"
        )

        assert_frame_equal(expected_features_df, features)
        np.testing.assert_array_equal(expected_targets, targets.values)

    @patch_all
    def test_main_correct_calls_for_success(
        self,
        mock_prepare_dataset: MagicMock,
        mock_write_to_csv: MagicMock,
        mock_read_from_csv: MagicMock,
        mock_os_path_join: MagicMock,
    ):
        mock_read_from_csv.return_value = pd.DataFrame([])
        mock_prepare_dataset.return_value = "a", "b"
        mock_os_path_join.return_value = ""

        main(
            self._raw_data_gbif_path,
            self._raw_data_species_path,
            self._species_name,
            self._prep_data_path,
            self._columns_to_drop,
            self._target_col_name,
            self._drop_nans,
            self._correlation_id,
        )

        mock_prepare_dataset.called_once()
        self.assertEqual(mock_read_from_csv.call_count, 2)
        self.assertEqual(mock_write_to_csv.call_count, 2)

    @patch_all
    def test_base_exception_correctly_handled(
        self,
        mock_prepare_dataset: MagicMock,
        mock_write_to_csv: MagicMock,
        mock_read_from_csv: MagicMock,
        mock_os_path_join: MagicMock,
    ):
        mock_read_from_csv.side_effect = Exception("error")
        mock_os_path_join.return_value = ""

        with self.assertRaises(SystemExit) as system_exit_exception:
            main(
                self._raw_data_gbif_path,
                self._raw_data_species_path,
                self._species_name,
                self._prep_data_path,
                self._columns_to_drop,
                self._target_col_name,
                self._drop_nans,
                self._correlation_id,
            )
        self.assertEqual(system_exit_exception.exception.code, 1)

        mock_prepare_dataset.called_once()
