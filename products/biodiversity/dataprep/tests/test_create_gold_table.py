# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import uuid
from functools import wraps
from typing import List
from unittest import TestCase
from unittest.mock import MagicMock, patch

from create_gold_table import create_gold_data_table, main

MOCK_WALK = [
    ("feature1", [], ["file1.csv", "file2.csv"]),
    ("feature2", [], ["file1.csv", "file2.csv"]),
    ("feature3", [], ["file1.csv", "file2.csv"]),
]


def patch_all(f):
    """
    Patches all of the library calls that are required to
    make the create_gold_data_table call succeed.
    Most objects are then asserted on in test.
    """

    @patch("os.walk")
    @patch("create_gold_table.path")
    @patch("create_gold_table.pd")
    @patch("create_gold_table.combine_data_tables")
    @wraps(f)
    def functor(*args, **kwargs):
        return f(*args, **kwargs)

    return functor


class TestCreateGoldDataset(TestCase):
    _gbif_extension = ".csv"
    _handle_nans = "fill"
    _fill_value = 1
    _drop_duplicates = True
    _gbif_feature_dir = "."
    _output_path = "./output.csv"
    _correlation_id = uuid.uuid4()

    @patch_all
    def test_create_gold_data_table_raises_exception_if_feature_dir_missing(
        self,
        mock_combine_data_tables: MagicMock,
        mock_pandas: MagicMock,
        mock_path: MagicMock,
        mock_walk: MagicMock,
    ):
        # Ensure the feature dir does not exist
        mock_path.exists.return_value = False

        with self.assertRaises(FileNotFoundError):
            self.__call_create_gold_data_table()

    @patch_all
    def test_create_gold_data_table_reads_data_with_pandas(
        self,
        mock_combine_data_tables: MagicMock,
        mock_pandas: MagicMock,
        mock_path: MagicMock,
        mock_walk: MagicMock,
    ):
        mock_path.exists.return_value = True
        mock_walk.return_value = MOCK_WALK

        self.__call_create_gold_data_table()

        assert mock_pandas.read_csv.called

    @patch_all
    def test_create_gold_data_table_combines_all_data(
        self,
        mock_combine_data_tables: MagicMock,
        mock_pandas: MagicMock,
        mock_path: MagicMock,
        mock_walk: MagicMock,
    ):
        mock_path.exists.return_value = True
        mock_walk.return_value = MOCK_WALK

        self.__call_create_gold_data_table()

        assert mock_combine_data_tables.called

    @patch_all
    def test_create_gold_data_table_writes_dataframe_to_csv(
        self,
        mock_combine_data_tables: MagicMock,
        mock_pandas: MagicMock,
        mock_path: MagicMock,
        mock_walk: MagicMock,
    ):
        mock_path.exists.return_value = True
        mock_walk.return_value = MOCK_WALK

        mock_dataframe = MagicMock()
        mock_combine_data_tables.return_value = mock_dataframe

        self.__call_create_gold_data_table()

        assert mock_dataframe.to_csv.called

    @patch_all
    def test_main_enforces_drop_duplicates_as_required(
        self,
        mock_combine_data_tables: MagicMock,
        mock_pandas: MagicMock,
        mock_path: MagicMock,
        mock_walk: MagicMock,
    ):
        self.__initialize_good_args()
        self._args.remove(f"--drop_duplicates={self._drop_duplicates}")

        with self.assertRaises(SystemExit) as system_exit_exception:
            main(self._args)
        self.assertEqual(system_exit_exception.exception.code, 2)

    @patch_all
    def test_main_enforces_gbif_feature_dir_as_required(
        self,
        mock_combine_data_tables: MagicMock,
        mock_pandas: MagicMock,
        mock_path: MagicMock,
        mock_walk: MagicMock,
    ):
        self.__initialize_good_args()
        self._args.remove(f"--gbif_feature_dir={self._gbif_feature_dir}")

        with self.assertRaises(SystemExit) as system_exit_exception:
            main(self._args)
        self.assertEqual(system_exit_exception.exception.code, 2)

    @patch_all
    def test_main_enforces_csv_output_path_as_required(
        self,
        mock_combine_data_tables: MagicMock,
        mock_pandas: MagicMock,
        mock_path: MagicMock,
        mock_walk: MagicMock,
    ):
        self.__initialize_good_args()
        self._args.remove(f"--csv_output_path={self._output_path}")

        with self.assertRaises(SystemExit) as system_exit_exception:
            main(self._args)
        self.assertEqual(system_exit_exception.exception.code, 2)

    @patch_all
    def test_main_enforces_fill_kind_and_fill_value_as_mutually_exclusive(
        self,
        mock_combine_data_tables: MagicMock,
        mock_pandas: MagicMock,
        mock_path: MagicMock,
        mock_walk: MagicMock,
    ):
        self.__initialize_good_args()
        self._args.append("--fill_kind=mean")

        with self.assertRaises(SystemExit) as system_exit_exception:
            main(self._args)
        self.assertEqual(system_exit_exception.exception.code, 2)

    @patch_all
    def test_main_enforces_one_of_fill_kind_and_fill_value_are_required(
        self,
        mock_combine_data_tables: MagicMock,
        mock_pandas: MagicMock,
        mock_path: MagicMock,
        mock_walk: MagicMock,
    ):
        self.__initialize_good_args()
        self._args.remove(f"--fill_value={self._fill_value}")

        with self.assertRaises(SystemExit) as system_exit_exception:
            main(self._args)
        self.assertEqual(system_exit_exception.exception.code, 2)

    def __call_create_gold_data_table(self):
        create_gold_data_table(
            gbif_extension=self._gbif_extension,
            drop_duplicates=self._drop_duplicates,
            correlation_id=self._correlation_id,
            csv_output_path=self._output_path,
            fill_value=self._fill_value,
            handle_nans=self._handle_nans,
        )

    def __initialize_good_args(self):
        self._args: List[str] = list()
        self._args.append(f"--handle_nans={self._handle_nans}")
        self._args.append(f"--fill_value={self._fill_value}")
        self._args.append(f"--drop_duplicates={self._drop_duplicates}")
        self._args.append(f"--gbif_feature_dir={self._gbif_feature_dir}")
        self._args.append(f"--csv_output_path={self._output_path}")
