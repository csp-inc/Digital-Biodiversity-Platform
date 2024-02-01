import json
import pickle
from unittest import TestCase
from unittest.mock import mock_open, patch

import pandas as pd
from lightgbm import LGBMClassifier
from pandas.testing import assert_frame_equal
from sklearn.base import BaseEstimator
from utils.read_utils import (
    load_model_from_pickle,
    lower_replace,
    read_from_config_file,
    read_from_csv,
)


class TestReadUtils(TestCase):
    # Test that reading config files works as expected.
    @patch("utils.read_utils.AzureLogger.log")
    def test_read_from_config_file(self, mock_azure_logger):
        mock_data_dict = {"a": 1, "b": 2, "c": 3}
        mock_json_object = json.dumps(mock_data_dict, indent=4)

        mock_open_object = mock_open(read_data=mock_json_object)

        with patch("builtins.open", mock_open_object):
            file_content = read_from_config_file("mock_yaml_file.yaml", mock_azure_logger)

        self.assertEqual(file_content, mock_data_dict)
        mock_azure_logger.called_once()

    # Test that reading models from pickle files works as expected.
    @patch("utils.read_utils.AzureLogger.log")
    def test_load_model_from_pickle(self, mock_azure_logger):
        mock_model = LGBMClassifier()
        mock_pickle_object = pickle.dumps(mock_model)
        mock_open_object = mock_open(read_data=mock_pickle_object)

        with patch("builtins.open", mock_open_object):
            file_content = load_model_from_pickle("mock_model_file.pkl", mock_azure_logger)

        self.assertIsInstance(file_content, BaseEstimator)
        mock_azure_logger.called_once()

    # Test that reading csv files as pandas dataframes works as expected.
    @patch("utils.read_utils.AzureLogger.log")
    def test_read_from_csvl(self, mock_azure_logger):
        mock_csv = """a_b_c,b*,c\nd,e,f"""
        mock_df_columns = ["a_b_c", "b", "c"]
        mock_df_rows = [["d", "e", "f"]]
        mock_df = pd.DataFrame(data=mock_df_rows, columns=mock_df_columns)
        mock_open_object = mock_open(read_data=mock_csv)

        with patch("builtins.open", mock_open_object):
            file_content = read_from_csv(
                "mock_data_file.csv", mock_azure_logger, index_column=None
            )
        assert_frame_equal(file_content, mock_df)
        mock_azure_logger.called_once()

    def test_lower_replace(self):
        string_to_format = " Jamaica BurTeo "

        string_formatted = lower_replace(string_to_format, " ", "-")

        self.assertEqual(string_formatted, "jamaica-burteo")
