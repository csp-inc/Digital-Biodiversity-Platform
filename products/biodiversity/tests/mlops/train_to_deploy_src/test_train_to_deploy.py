# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import os
import tempfile
from unittest import TestCase
from unittest.mock import Mock, patch
from uuid import uuid4

import pandas as pd
import pytest
from mlops.train_to_deploy_src.train_to_deploy import main


class TestTrainToDeploy(TestCase):
    _args = {
        "prep_data_path": os.path.join(
            "./products/biodiversity/tests/mlops", "train_src/test_data_for_training/"
        ),
        "best_parameters_path": "./products/biodiversity/tests/mlops/score_src/",
        "model_path": "./fake",
        "model_metadata": "./fake",
        "correlation_id": str(uuid4()),
    }
    _log_return_value = {"run_id": 0, "run_uri": 1}

    @pytest.mark.integtest
    @patch("mlops.train_to_deploy_src.train_to_deploy.mlflow.lightgbm.log_model")
    def test_train_to_deploy(self, mock_mlflow_log_model):
        mock_obj = Mock()
        mock_obj.run_id = "0"
        mock_obj.model_uri = "1"
        mock_mlflow_log_model.return_value = mock_obj
        with tempfile.TemporaryDirectory() as tmpdir:
            _, mock_file_path = tempfile.mkstemp()

            self._args["model_path"] = tmpdir
            self._args["model_metadata"] = mock_file_path

            main(**self._args)

            files = os.listdir(tmpdir)

            self.assertEqual(
                sorted(files),
                [
                    "MLmodel",
                    "conda.yaml",
                    "model.pkl",
                    "python_env.yaml",
                    "requirements.txt",
                ],
            )

    @patch("mlops.train_to_deploy_src.train_to_deploy.read_from_csv")
    @patch("mlops.train_to_deploy_src.train_to_deploy.read_from_config_file")
    @patch("mlops.train_to_deploy_src.train_to_deploy.LGBMClassifier")
    @patch("mlops.train_to_deploy_src.train_to_deploy.mlflow")
    @patch("mlops.train_to_deploy_src.train_to_deploy.write_to_json")
    @patch("mlops.train_to_deploy_src.train_to_deploy.mlflow.lightgbm.log_model")
    def test_main_success(
        self,
        mock_mlflow_log_model,
        mock_write_to_json,
        mock_mlflow,
        mock_lgbmclassifier,
        mock_read_from_config_file,
        mock_read_from_csv,
    ):
        mock_mlflow_log_model.return_value = self._log_return_value
        mock_model = mock_lgbmclassifier
        mock_model.fit.return_value = ""
        mock_read_from_csv.return_value = pd.DataFrame([])
        mock_lgbmclassifier.return_value = mock_model
        mock_read_from_config_file.return_value = {}

        main(**self._args)

        mock_read_from_config_file.caled_once()
        mock_lgbmclassifier.called_once()
        mock_model.fit.called_once()
        mock_write_to_json.called_once()
        mock_mlflow_log_model.called_once()
        mock_mlflow.lightgmb.save_model.called_once()
        self.assertEqual(mock_read_from_csv.call_count, 2)

    @patch("mlops.train_to_deploy_src.train_to_deploy.read_from_csv")
    def test_main_handles_base_exception(self, mock_read_from_csv):
        mock_read_from_csv.side_effect = Exception("error")

        with self.assertRaises(SystemExit) as system_exit_exception:
            main(**self._args)
        self.assertEqual(system_exit_exception.exception.code, 1)
