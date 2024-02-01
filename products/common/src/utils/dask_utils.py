import logging
import socket
import uuid

from azure_logger import AzureLogger
from dask.distributed import Client
from dask_mpi import initialize


def log_dask_dashboard_address(dask_client: Client, azure_logger: AzureLogger) -> None:
    """
    Retrieves the dashboard port for the local Dask client.

    Parameters
    ----------
    client: Client
        Dask client.
    azure_logger: AzureLogger
        AzureLogger instance that communicates with AppInsights.

    """

    host = dask_client.run_on_scheduler(socket.gethostname)
    host_addr = socket.gethostbyname(str(host))
    port = dask_client.scheduler_info()["services"]["dashboard"]
    azure_logger.log("Dask initialized", **{"local cluster": f"{port}:{host_addr}:{port}"})


def initialize_dask_cluster(
    use_local_cluster: bool = False,
    correlation_id: uuid.UUID = uuid.uuid4(),
) -> None:
    """
    Initializes a local or distributed Dask-MPI cluster.

    Parameters
    ----------
    use_local_cluster: boolean
        Boolean which indicates whether a local or distributed version of Dask is used.
        By default the distributed cluster is used.
    correlation_id: uuid.uuid4
        Correlation id used for tracking transactions in Appinsights.

    """
    azure_logger = AzureLogger(correlation_id=correlation_id, level=logging.DEBUG)

    if use_local_cluster:
        azure_logger.log("Initializing local cluster")
        dask_client = Client(processes=False)
        log_dask_dashboard_address(dask_client, azure_logger)
        azure_logger.log("Local cluster successfully initialized")

    else:
        azure_logger.log("Initializing MPI cluster")
        initialize()
        dask_client = Client()
        azure_logger.log("MPI cluster successfully initialized")
