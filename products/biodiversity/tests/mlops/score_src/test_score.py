# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import logging
import os
import tempfile
from unittest import TestCase
from unittest.mock import patch
from uuid import uuid4

import pandas as pd
import pytest
from azure_logger import AzureLogger
from mlops.score_src.score import evaluate, main, write_results
from pandas.testing import assert_frame_equal
from utils.read_utils import read_from_config_file


class TestScore(TestCase):
    _TEST_METRICS = {
        "roc_auc": 0.5,
        "clf_report": {
            "0": {
                "precision": 0.5,
                "recall": 1.0,
                "f1-score": 0.6666666666666666,
                "support": 2,
            },
            "1": {"precision": 0.0, "recall": 0.0, "f1-score": 0.0, "support": 2},
            "accuracy": 0.5,
            "macro avg": {
                "precision": 0.25,
                "recall": 0.5,
                "f1-score": 0.3333333333333333,
                "support": 4,
            },
            "weighted avg": {
                "precision": 0.25,
                "recall": 0.5,
                "f1-score": 0.3333333333333333,
                "support": 4,
            },
        },
        "confusion_m": {
            "ABSENT": {"ABSENT": 2, "PRESENT": 2},
            "PRESENT": {"ABSENT": 0, "PRESENT": 0},
        },
    }

    _target_test = [0, 1, 0, 1]
    _target_pred = [0, 0, 0, 0]
    _classes = ["ABSENT", "PRESENT"]
    _confusion_m_df_mock = pd.DataFrame(
        _TEST_METRICS["confusion_m"], index=_classes, columns=_classes
    )

    @patch("mlops.score_src.score.mlflow")
    def test_evaluate(self, mock_mlflow):
        metrics, confusion_m_df, clf_report = evaluate(
            self._target_test, self._target_pred, self._classes
        )

        self.assertEqual(metrics, self._TEST_METRICS)
        self.assertEqual(clf_report, self._TEST_METRICS["clf_report"])
        assert_frame_equal(confusion_m_df, self._confusion_m_df_mock)

    def test_write_results(self):
        azure_logger = AzureLogger(correlation_id=uuid4(), level=logging.DEBUG)
        with tempfile.TemporaryDirectory() as tmpdir:
            write_results(
                self._TEST_METRICS,
                self._confusion_m_df_mock,
                self._TEST_METRICS["clf_report"],
                output_folder=tmpdir,
                azure_logger=azure_logger,
            )
            files = os.listdir(tmpdir)
            self.assertEqual(len(files), 3)
            self.assertEqual(
                sorted(files), sorted(["cfm.png", "clf_report.png", "metrics.json"])
            )

    @patch("mlops.score_src.score.write_results")
    @patch("mlops.score_src.score.evaluate")
    @patch("mlops.score_src.score.LGBMClassifier")
    @patch("mlops.score_src.score.read_from_config_file")
    @patch("mlops.score_src.score.read_from_csv")
    @patch("mlops.score_src.score.os.path.join")
    @patch("mlops.score_src.score.mlflow")
    def test_main_success(
        self,
        mock_mlflow,
        mock_os,
        mock_read_from_csv,
        mock_read_from_config_file,
        mock_lgbmclassifier,
        mock_evaluate,
        mock_write_results,
    ):
        mock_model = mock_lgbmclassifier
        mock_model.fit.return_value = ""
        mock_model.predict.return_value = []
        mock_read_from_csv.return_value = pd.DataFrame([])
        mock_os.return_value = "fake"
        mock_read_from_config_file.return_value = {}
        mock_lgbmclassifier.return_value = ""
        mock_evaluate.return_value = {}, pd.DataFrame([]), {}

        main("fake", "fake", "fake", None)

        self.assertEqual(mock_read_from_csv.call_count, 4)
        self.assertEqual(mock_os.call_count, 5)
        mock_read_from_config_file.called_once()
        mock_lgbmclassifier.called_once()
        mock_model.fit.called_once()
        mock_model.predict.called_once()
        mock_evaluate.called_once()
        mock_write_results.called_once()

    @pytest.mark.integtest
    def test_main_integration(self):
        args = {
            "scoring_data_path": os.path.join(
                "./products/biodiversity/tests/mlops", "score_src/test_data_for_scoring/"
            ),
            "best_parameters_path": "./products/biodiversity/tests/mlops/score_src/",
            "correlation_id": None,
        }
        azure_logger = AzureLogger(correlation_id=uuid4(), level=logging.DEBUG)

        with tempfile.TemporaryDirectory() as tmpdir:
            args["scores_path"] = tmpdir
            main(**args)
            files = os.listdir(tmpdir)
            metrics_dict = read_from_config_file(
                os.path.join(tmpdir, "metrics.json"), azure_logger
            )
            self.assertEqual(
                sorted(metrics_dict.keys()), ["clf_report", "confusion_m", "roc_auc"]
            )
            self.assertEqual(len(files), 3)
            self.assertEqual(
                sorted(files), sorted(["cfm.png", "clf_report.png", "metrics.json"])
            )
