# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import os
import unittest
from functools import wraps
from unittest.mock import MagicMock, patch

from azure.ai.ml.entities import BatchJob
from perform_inference import perform_inference


def patch_all(f):
    """
    Patches all of the library calls that are required to
    make the perform_inference call succeed. Most objects are then
    asserted on in test.
    """

    @patch("perform_inference.pd.DataFrame.to_csv")
    @patch("perform_inference.MLClient")
    @wraps(f)
    def functor(*args, **kwargs):
        return f(*args, **kwargs)

    return functor


class TestInference(unittest.TestCase):
    _site_name = "cherrypt"
    _valid_species_list_csv = "products/biodiversity/inference/tests/data/species_list.csv"
    _valid_batch_input_path = "testpath"
    _valid_merged_df_path = "products/biodiversity/inference/tests/data"
    _valid_output_path = "products/biodiversity/inference/tests/data/output_test.csv"
    _max_wait_time = 30
    _subscription_id = "test"
    _resource_group = "test"
    _workspace = "test"

    def clean_up(self) -> None:
        melanitta_file = "./cherrypt/melanitta-perspicillata/predictions.csv"
        burteo_file = "./cherrypt/burteo-jamaicensis/predictions.csv"
        if os.path.exists(melanitta_file):
            os.system(f"rm {melanitta_file}")
        if os.path.exists(burteo_file):
            os.system(f"rm {burteo_file}")

    def init_ml_client_mock(self) -> None:
        self._ml_client = MagicMock()
        self._batch_endpoints = MagicMock()
        self._batch_endpoints.invoke.side_effect = [
            BatchJob(name="first"),
            BatchJob(name="second"),
        ]

        self._jobs = MagicMock()
        job = MagicMock()
        job.status = "Completed"
        self._jobs.get.return_value = job

        def _fake_download(**kwargs):
            os.system(
                "cp products/biodiversity/inference/tests/data/predictions.csv "
                "./cherrypt/melanitta-perspicillata/predictions.csv"
            )
            os.system(
                "cp products/biodiversity/inference/tests/data/predictions.csv "
                "./cherrypt/burteo-jamaicensis/predictions.csv"
            )

        self._jobs.download.side_effect = _fake_download

        self._ml_client.batch_endpoints = self._batch_endpoints
        self._ml_client.jobs = self._jobs

    @patch_all
    def test_inference_creates_ml_client(
        self, ml_client_module: MagicMock, to_csv: MagicMock
    ):
        # arrange
        self.init_ml_client_mock()
        ml_client_module.return_value = self._ml_client

        # act
        perform_inference(
            self._site_name,
            self._valid_species_list_csv,
            self._valid_batch_input_path,
            self._valid_merged_df_path,
            self._valid_output_path,
            self._max_wait_time,
            self._subscription_id,
            self._resource_group,
            self._workspace,
        )

        # assert
        self.assertTrue(ml_client_module.called)
        self.assertEqual(ml_client_module.call_args.args[1], self._subscription_id)
        self.assertEqual(ml_client_module.call_args.args[2], self._resource_group)
        self.assertEqual(ml_client_module.call_args.args[3], self._workspace)

        self.clean_up()

    @patch_all
    def test_inference_invokes_batch_endpoints(
        self, ml_client_module: MagicMock, to_csv: MagicMock
    ):
        # arrange
        self.init_ml_client_mock()
        ml_client_module.return_value = self._ml_client

        # act
        perform_inference(
            self._site_name,
            self._valid_species_list_csv,
            self._valid_batch_input_path,
            self._valid_merged_df_path,
            self._valid_output_path,
            self._max_wait_time,
            self._subscription_id,
            self._resource_group,
            self._workspace,
        )

        # assert
        self.assertEqual(self._batch_endpoints.invoke.call_count, 2)

        self.clean_up()

    @patch_all
    def test_inference_downloads_predicitions(
        self, ml_client_module: MagicMock, to_csv: MagicMock
    ):
        # arrange
        self.init_ml_client_mock()
        ml_client_module.return_value = self._ml_client

        # act
        perform_inference(
            self._site_name,
            self._valid_species_list_csv,
            self._valid_batch_input_path,
            self._valid_merged_df_path,
            self._valid_output_path,
            self._max_wait_time,
            self._subscription_id,
            self._resource_group,
            self._workspace,
        )

        # assert
        self.assertEqual(self._jobs.download.call_count, 2)

        self.clean_up()

    @patch_all
    def test_inference_writes_results_to_csv(
        self, ml_client_module: MagicMock, to_csv: MagicMock
    ):
        # arrange
        self.init_ml_client_mock()
        ml_client_module.return_value = self._ml_client

        # act
        perform_inference(
            self._site_name,
            self._valid_species_list_csv,
            self._valid_batch_input_path,
            self._valid_merged_df_path,
            self._valid_output_path,
            self._max_wait_time,
            self._subscription_id,
            self._resource_group,
            self._workspace,
        )

        # assert
        self.assertTrue(to_csv.called)
        output_file_path = to_csv.call_args.args[0]
        self.assertTrue(output_file_path.__contains__(self._valid_output_path))

        self.clean_up()

    @patch_all
    def test_inference_failed_job_raises_exception(
        self, ml_client_module: MagicMock, to_csv: MagicMock
    ):
        # arrange
        self.init_ml_client_mock()
        ml_client_module.return_value = self._ml_client

        job = MagicMock()
        job.status = "Failed"
        self._jobs.get.return_value = job

        # act
        with self.assertRaises(Exception):
            perform_inference(
                self._site_name,
                self._valid_species_list_csv,
                self._valid_batch_input_path,
                self._valid_merged_df_path,
                self._valid_output_path,
                self._max_wait_time,
                self._subscription_id,
                self._resource_group,
                self._workspace,
            )

        # assert
        self.clean_up()
