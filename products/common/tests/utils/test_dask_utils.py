from unittest import TestCase
from unittest.mock import patch

from utils.dask_utils import initialize_dask_cluster, log_dask_dashboard_address


class TestDaskUtils(TestCase):
    # Test that the initialization of the local cluster works as expected.
    @patch("utils.dask_utils.log_dask_dashboard_address")
    @patch("utils.dask_utils.Client")
    @patch("utils.dask_utils.AzureLogger.log")
    def test_success_local_cluster(
        self, mock_azure_logger_log, mock_client, mock_log_dask_dashboard_address
    ):
        initialize_dask_cluster(True)

        mock_log_dask_dashboard_address.assert_called_once()
        mock_client.assert_called_once()
        self.assertEqual(mock_azure_logger_log.call_count, 2)

    # Test that the distributed MPI cluster initialization works as expected.
    @patch("utils.dask_utils.initialize")
    @patch("utils.dask_utils.Client")
    @patch("utils.dask_utils.AzureLogger.log")
    def test_success_distributed_cluster(
        self, mock_azure_logger_log, mock_client, mock_initialize
    ):
        initialize_dask_cluster()

        mock_client.assert_called_once()
        mock_initialize.assert_called_once()
        self.assertEqual(mock_azure_logger_log.call_count, 2)

    # Test the successful logging of the dashboard address.
    @patch("utils.dask_utils.Client")
    @patch("utils.dask_utils.socket")
    @patch("utils.dask_utils.AzureLogger")
    def test_log_dask_dashboard_address_success(
        self, mock_azure_logger, mock_socket, mock_client
    ):
        port_value = "12"
        port = {"services": {"dashboard": port_value}}
        socket = "1111"
        first_logger_arg = "Dask initialized"
        second_logger_arg = {"local cluster": f"{port_value}:{socket}:{port_value}"}
        mock_client.scheduler_info.return_value = port
        mock_socket.gethostbyname.return_value = socket
        mock_azure_logger.log.return_value = None

        log_dask_dashboard_address(mock_client, mock_azure_logger)

        mock_socket.gethostbyname.assert_called_once()
        mock_client.run_on_scheduler.assert_called_once()
        mock_client.scheduler_info.assert_called_once()
        self.assertEqual(first_logger_arg, mock_azure_logger.log.call_args[0][0])
        self.assertEqual(second_logger_arg, mock_azure_logger.log.call_args[1])
