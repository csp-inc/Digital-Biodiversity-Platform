# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import unittest
from functools import wraps
from typing import List
from unittest.mock import MagicMock, PropertyMock, patch

from mlclient_utils import wait_until_job_finished


def patch_all(f):
    """
    Patches all of the library calls that are required to
    make the wait_until_job_finished call succeed. Most objects are then
    asserted on in test.
    """

    @patch("mlclient_utils.time")
    @wraps(f)
    def functor(*args, **kwargs):
        return f(*args, **kwargs)

    return functor


class TestMlclientUtils(unittest.TestCase):
    _job_name = "test"
    _max_wait_time = 15

    def init_mocks(self, job_status: List[str]) -> None:
        self._ml_client = MagicMock()

        self._jobs = MagicMock()
        job = MagicMock()
        self._status_mock = PropertyMock()
        self._status_mock.side_effect = job_status
        type(job).status = self._status_mock
        self._jobs.get.return_value = job

        self._ml_client.jobs = self._jobs

    @patch_all
    def test_job_completes(self, time: MagicMock):
        # arrange
        self.init_mocks(["Running", "Completed"])

        # act
        wait_until_job_finished(self._ml_client, self._job_name, self._max_wait_time)

        # assert
        self.assertEqual(self._jobs.get.call_count, 2)
        self.assertEqual(self._status_mock.call_count, 2)

    @patch_all
    def test_failed_job_exception(self, time: MagicMock):
        # arrange
        self.init_mocks(["Running", "Failed"])

        # act
        with self.assertRaises(Exception):
            wait_until_job_finished(self._ml_client, self._job_name, self._max_wait_time)

    @patch_all
    def test_time_out_exception(self, time: MagicMock):
        # arrange
        self.init_mocks(["Running"])

        # act
        with self.assertRaises(Exception):
            wait_until_job_finished(self._ml_client, self._job_name, self._max_wait_time)
