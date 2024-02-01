# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import logging
import os
from unittest import TestCase
from unittest.mock import patch
from uuid import uuid4

import numpy as np
from azure_logger import AzureLogger
from mlops.scoring_src.scoring import init, run
from utils.read_utils import load_model_from_pickle


class TestScoring(TestCase):
    @patch("mlflow.lightgbm.load_model")
    def test_correct_run(self, mock_mlflow):
        os.environ["AZUREML_MODEL_DIR"] = "1"
        _azure_logger = AzureLogger(correlation_id=uuid4(), level=logging.DEBUG)
        mock_model_path = "./products/biodiversity/tests/mlops/scoring_src/model.pkl"

        model = load_model_from_pickle(mock_model_path, _azure_logger)

        mock_mlflow.return_value = model

        batch_data_path = (
            "./products/biodiversity/tests/mlops/scoring_src/test_data_for_scoring"
        )
        batch_files = [
            os.path.join(batch_data_path, "file_1.csv"),
            os.path.join(batch_data_path, "file_2.csv"),
        ]

        init()

        results = run(batch_files)

        self.assertEqual(51, len(results))
        np.testing.assert_equal(["file_1.csv", "file_2.csv"], np.unique(results["file"]))
        np.testing.assert_equal(
            ["file", "predictions_ABSENT", "predictions_PRESENT"], results.columns.values
        )
