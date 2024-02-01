# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import logging
import time
import uuid

from azure.ai.ml import MLClient
from azure_logger import AzureLogger

running_status = [
    "NotStarted",
    "Queued",
    "Starting",
    "Preparing",
    "Running",
    "Finalizing",
    "Provisioning",
    "CancelRequested",
    "Failed",
    "Canceled",
    "NotResponding",
]

stopped_status = ["Failed", "Canceled"]


def wait_until_job_finished(
    ml_client: MLClient,
    job_name: str,
    max_wait_time: int,
    azure_logger: AzureLogger = AzureLogger(
        correlation_id=uuid.uuid4(), level=logging.DEBUG
    ),
) -> None:
    """
    Query job status until job is completed. If the max time is reached
    without completetion, an exception is thrown.

    Parameters
    ----------
    ml_client: str
        MLClient to query the job on.
    job_name: str
        Name of the job to query.
    max_wait_time: int
        The maximum amount of time in seconds to wait for a job to complete.
    azure_logger: str
        Azure logger.
    Returns
    -------
        None
    """
    interval = 10
    current_wait_time = 0
    status = ml_client.jobs.get(job_name).status

    while status in running_status:
        if (current_wait_time <= max_wait_time) and (status not in stopped_status):
            azure_logger.log(
                f"Job not yet complete. Current wait time {current_wait_time}. "
                f"trying again in {interval} seconds",
                **{"job_name:": job_name, "status": status},
            )
            time.sleep(interval)
            current_wait_time = current_wait_time + interval
            status = ml_client.jobs.get(job_name).status
        else:
            break

    if status == "Completed" or status == "Finished":
        azure_logger.event("Job completed", **{"job_name": job_name})
    else:
        azure_logger.event("Job not completed", **{"job_name": job_name, "status": status})
        raise Exception(
            f"Sorry, the job {job_name} has not completed sucessfully with status {status}"
        )
