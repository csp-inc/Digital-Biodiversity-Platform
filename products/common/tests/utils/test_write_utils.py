from unittest import TestCase
from unittest.mock import mock_open, patch

import numpy as np
import pandas as pd
from lightgbm import LGBMClassifier
from utils.write_utils import (
    convert_dictionary_numpy_values_to_json_readable,
    convert_to_json,
    write_model_to_pickle,
    write_to_csv,
    write_to_json,
)


class TestWriteUtils(TestCase):
    # Test json conversion works as expected.
    def test_convert_to_json(self):
        mock_data_dict = {"a": 1, "b": 2, "c": 3}
        expected_output = '{\n    "a": 1,\n    "b": 2,\n    "c": 3\n}'

        json_object = convert_to_json(mock_data_dict)

        self.assertEqual(json_object, expected_output)

    # Test that writing a file to json works as expected.
    @patch("utils.write_utils.json")
    @patch("utils.write_utils.AzureLogger.log")
    def test_write_to_json(self, mock_azure_logger, mock_json):
        mock_data_json = '{"a": 1, "b": 2, "c": 3}'
        mock_file_path = "fake/file/json_path"

        with patch("builtins.open", mock_open()) as json_file:
            write_to_json(mock_file_path, mock_data_json, mock_azure_logger)

            mock_json.dump.assert_called_with(mock_data_json, json_file(), indent=4)

        mock_azure_logger.called_once()

    # Test that writing a model is executed succesfully.
    @patch("utils.write_utils.pickle")
    @patch("utils.write_utils.AzureLogger.log")
    def test_write_model_to_pickle(self, mock_azure_logger, mock_pickle):
        mock_model = LGBMClassifier()
        mock_file_path = "fake/file/model_path"

        with patch("builtins.open", mock_open()) as model_file:
            write_model_to_pickle(mock_model, mock_file_path, mock_azure_logger)
            mock_pickle.dump.assert_called_with(mock_model, model_file())

        mock_azure_logger.called_once()

    # Test that writing to a csv file works as expected.
    @patch("utils.write_utils.AzureLogger.log")
    def test_write_to_csv(self, mock_azure_logger):
        mock_dataframe = pd.DataFrame([["a", "b", "c"]])

        with patch("pandas.DataFrame.to_csv") as to_csv_mock:
            write_to_csv(mock_dataframe, "pandas_dataframe.csv", mock_azure_logger)
            to_csv_mock.assert_called_with("pandas_dataframe.csv")

        mock_azure_logger.called_once()

    def test_convert_dictionary_numpy_values_to_json_readable(self):
        dictionary = {"a": np.float64(12.23), "b": np.int64(10)}

        converted_dictionary = convert_dictionary_numpy_values_to_json_readable(dictionary)

        self.assertIsInstance(converted_dictionary["a"], float)
        self.assertIsInstance(converted_dictionary["b"], int)
