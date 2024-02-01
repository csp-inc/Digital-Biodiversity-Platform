# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import os
from unittest import TestCase
from unittest.mock import patch
from uuid import uuid4

import pandas as pd
import pytest
from mlops.train_src.train import main


class TestTrain(TestCase):
    _args = {
        "input_path": os.path.join(
            "./products/biodiversity/tests", "mlops/train_src/test_data_for_training/"
        ),
        "parameters_path": "./mlops/biodiversity/config/lgbm_params.yml",
        "best_parameters_path": "./fake",
        "scoring_data_path": "./fake",
        "train_fraction": 0.9,
        "random_seed": 42,
        "maximum_evaluations": 5,
        "hyperparameter_algorithm": "tpe",
        "correlation_id": str(uuid4()),
        "number_of_folds": 2,
        "verbose": -1,
    }

    @pytest.mark.integtest
    @patch("mlops.train_src.train.write_to_json")
    @patch("mlops.train_src.train.write_to_csv")
    @patch("mlops.train_src.train.trials_mlflow_logging")
    def test_training(
        self, mock_trials_mlflow_logging, mock_write_to_csv, mock_write_to_json
    ):
        main(**self._args)

        best_parameter_space = mock_write_to_json.call_args_list[0][1]["data"]
        number_of_parameters = len(best_parameter_space)
        self.assertIn("boosting_type", best_parameter_space.keys())
        self.assertIn("lambda_l2", best_parameter_space.keys())
        self.assertEqual(number_of_parameters, 14)

    @patch("mlops.train_src.train.read_from_csv")
    @patch("mlops.train_src.train.split_train_test")
    @patch("mlops.train_src.train.parse_parameter_space_lgbm")
    @patch("mlops.train_src.train.partial")
    @patch("mlops.train_src.train.hyperparameter_algorithm_parsing")
    @patch("mlops.train_src.train.run_hyperoptimization")
    @patch("mlops.train_src.train.convert_dictionary_numpy_values_to_json_readable")
    @patch("mlops.train_src.train.write_to_json")
    @patch("mlops.train_src.train.write_to_csv")
    @patch("mlops.train_src.train.trials_mlflow_logging")
    def test_main_success(
        self,
        mock_trials_mlflow_logging,
        mock_write_to_csv,
        mock_write_to_json,
        mock_conversion,
        mock_run_hyperoptimization,
        mock_hyperparameter_algorithm_parsing,
        mock_partial,
        mock_parse_parameter_space_lgbm,
        mock_split_train_test,
        mock_read_from_csv,
    ):
        mock_read_from_csv.return_value = pd.DataFrame([])
        mock_split_train_test.return_value = "", "", "", ""
        mock_parse_parameter_space_lgbm.return_value = {}, {}
        mock_partial.return_value = ""
        mock_conversion.return_value = {}
        mock_hyperparameter_algorithm_parsing.return_value = ""
        mock_run_hyperoptimization.return_value = (
            {"early_stopping_rounds": 0, "a": 2},
            {"hello": 1},
        )

        main(**self._args)

        mock_write_to_json.caled_once()
        mock_conversion.called_once()
        mock_partial.called_once()
        mock_hyperparameter_algorithm_parsing.called_once()
        mock_run_hyperoptimization.called_once()
        mock_split_train_test.called_once()
        self.assertEqual(mock_read_from_csv.call_count, 2)
        self.assertEqual(mock_write_to_csv.call_count, 4)

    @patch("mlops.train_src.train.read_from_csv")
    def test_main_handles_base_exception(self, mock_read_from_csv):
        mock_read_from_csv.side_effect = Exception("error")

        with self.assertRaises(SystemExit) as system_exit_exception:
            main(**self._args)
        self.assertEqual(system_exit_exception.exception.code, 1)
