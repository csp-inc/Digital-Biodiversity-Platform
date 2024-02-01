# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import logging
from unittest import TestCase
from unittest.mock import patch
from uuid import uuid4

from azure_logger import AzureLogger
from hyperopt import Trials, tpe
from models.parameters import (
    hyperparameter_algorithm_parsing,
    parse_parameter_space_lgbm,
    run_hyperoptimization,
    trials_mlflow_logging,
)

_mock_trial = [
    {
        "result": {"loss": 0.6941515162774065, "status": "ok"},
        "misc": {
            "vals": {
                "lambda_l1": [0.1537986861870591],
                "lambda_l2": [0.40051613334770825],
                "learning_rate": [0.13434122460465764],
                "max_depth": [22],
                "min_data_in_leaf": [71],
            }
        },
    },
    {
        "result": {"loss": 0.6937546825972734, "status": "ok"},
        "misc": {
            "vals": {
                "lambda_l1": [0.8774853059341279],
                "lambda_l2": [0.3270095794002001],
                "learning_rate": [0.09880379493663562],
                "max_depth": [16],
                "min_data_in_leaf": [79],
            }
        },
    },
]


class MockTrial:
    def __init__(self):
        self.trials = _mock_trial


class TestParameters(TestCase):
    _params = {"random_seed": 42}
    _azure_logger = AzureLogger(correlation_id=uuid4(), level=logging.DEBUG)

    def _objective(self):
        return lambda _: 0

    def test_run_hyperopt_no_trial_specified(self):
        best, trials = run_hyperoptimization(self._params, self._objective())
        self.assertIsInstance(best, dict)
        self.assertEqual(len(best), 0)
        self.assertIsNotNone(trials)

    def test_run_hyperopt_with_trial_specified(self):
        trials = Trials()

        best, trials = run_hyperoptimization(self._params, self._objective(), trials=trials)

        self.assertIsInstance(best, dict)
        self.assertEqual(len(best), 0)
        self.assertIsNotNone(trials)

    def test_parse_parameter_space_lgbm(self):
        parameters_path = "./mlops/biodiversity/config/lgbm_params.yml"

        parameters, parameters_to_tune = parse_parameter_space_lgbm(
            parameters_path, self._azure_logger
        )

        self.assertIsInstance(parameters, dict)
        self.assertIsInstance(parameters_to_tune, dict)

    def test_hyperparameter_algorithm_parsing_tpe(self):
        algorithm = "tpe"

        algorithm_object = hyperparameter_algorithm_parsing(algorithm, self._azure_logger)

        self.assertEqual(algorithm_object, tpe)

    def test_hyperparameter_algorithm_parsing_wrong_name(self):
        algorithm = "random"

        with self.assertRaises(Exception) as context:
            hyperparameter_algorithm_parsing(algorithm, self._azure_logger)

        self.assertIn("Invalid hyperoptimization algorithm", context.exception.args[0])

    @patch("models.parameters.mlflow.log_metrics")
    @patch("models.parameters.mlflow.log_metric")
    def test_trials_mlflow_logging(self, mock_mlflow_metric, mock_mlflow_metrics):
        mock_trials = MockTrial()

        trials_mlflow_logging(mock_trials)

        self.assertEqual(
            mock_mlflow_metrics.call_args_list[0][0][0]["lambda_l1"], 0.1537986861870591
        )
        self.assertEqual(mock_mlflow_metric.call_args_list[0][0][1], 0.6941515162774065)
        self.assertEqual(mock_mlflow_metric.call_count, 2)
        self.assertEqual(mock_mlflow_metrics.call_count, 2)
