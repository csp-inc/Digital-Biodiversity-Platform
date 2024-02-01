import logging
import uuid

from pytest import MonkeyPatch

from ..src import azure_logger
from ..tests.test_support import mock_log_handler


class TestAzureLogger:
    def setup_method(self):
        self.__monkeypatch = MonkeyPatch()
        self.__mock_log_handler = mock_log_handler.MockLogHandler(level=logging.INFO)
        self.__mock_log_handler.name = "mock_logger"

    def teardown_method(self):
        self.__monkeypatch.undo()
        self.__mock_log_handler.clear_records()

    # Creates a default instance of Azure_Logger, usable in most test.
    # Tests can over write it with bespoke versions if required.
    # __logger = azure_logger.AzureLogger("test logger")

    def __set_connection_string(self):
        self.__monkeypatch.setenv(
            "APPLICATIONINSIGHTS_CONNECTION_STRING",
            "InstrumentationKey=00000000-0000-0000-0000-000000000000;"
            "IngestionEndpoint=https://fake.com/;"
            "LiveEndpoint=https://fake.com/",
        )

    # When the correlation id is passed as a parameter, it should be
    # set as a property on the returned Azure_Logger instance.
    def test_constructor_sets_correlation_id_on_logger(self):
        correlation_id = uuid.uuid4()
        self.__set_connection_string()
        logger = azure_logger.AzureLogger(correlation_id=correlation_id)

        assert logger.get_correlation_id() is correlation_id

    # The constructor should log a warning if the App Insights connection string
    # is missing
    def test_constructor_logs_warning_if_application_insights_connection_string_is_missing(
        self,
    ):
        # Remove the connection string from the environment variables, if it exists
        self.__monkeypatch.delenv("APPLICATIONINSIGHTS_CONNECTION_STRING", False)

        self.__mock_log_handler.setLevel(logging.WARNING)

        azure_logger.AzureLogger(handlers=[self.__mock_log_handler])

        found = [
            x
            for x in self.__mock_log_handler.get_records()
            if "Environment variable APPLICATIONINSIGHTS_CONNECTION_STRING not"
            " set or is invalid, local logging only." in x
        ]

        assert found != []

    # The create_measureFloat function should return a configured MeasureFloat.
    def test_create_measurefloat_returns_configured_measure(self):
        self.__set_connection_string()
        logger = azure_logger.AzureLogger()
        measure = logger.create_measure_float(
            "name",
            "description",
            "m",
            azure_logger.AzureLogger.MetricAggregationType.CountAggregation,
        )

        assert measure.name == "name"
        assert measure.description == "description"
        assert measure.unit == "m"

    # The create_measureInt function should return a configured MeasureInt.
    def test_create_measureint_returns_configured_measure(self):
        logger = azure_logger.AzureLogger()
        measure = logger.create_measure_int(
            "name",
            "description",
            "m",
            azure_logger.AzureLogger.MetricAggregationType.CountAggregation,
        )

        assert measure.name == "name"
        assert measure.description == "description"
        assert measure.unit == "m"

    # The Event function should log the event name.
    def test_event_logs_event_name_as_info(self):
        self.__set_connection_string()
        logger = azure_logger.AzureLogger(handlers=[self.__mock_log_handler])
        event_name = "event_name"

        logger.event(event_name)

        found = [x for x in self.__mock_log_handler.get_records() if event_name in x]

        assert found != []

    # The Event function should log the custom dimensions.
    def test_event_logs_custom_dimensions_as_info(self):
        self.__set_connection_string()
        logger = azure_logger.AzureLogger(handlers=[self.__mock_log_handler])
        event_name = "event_name"

        logger.event(event_name, **{"property": "property_value"})

        found = [
            x
            for x in self.__mock_log_handler.get_records()
            if "property: property_value" in x
        ]

        assert found != []

    # The Event function should log the event name with correlation id
    def test_event_logs_event_name_as_info_with_correlation_id(self):
        self.__set_connection_string()

        correlation_id = uuid.uuid4()

        logger = azure_logger.AzureLogger(
            correlation_id=correlation_id,
            handlers=[self.__mock_log_handler],
        )

        event_name = "event_name"

        logger.event(event_name)

        found = [x for x in self.__mock_log_handler.get_records() if event_name in x]

        assert found != []
        assert correlation_id.hex in found[0]

    # The Exception function logs.
    def test_exception_logs(self):
        self.__set_connection_string()
        logger = azure_logger.AzureLogger(handlers=[self.__mock_log_handler])
        exception = KeyError("KeyError")

        logger.exception(exception, message="message")

        found = [x for x in self.__mock_log_handler.get_records() if "message" in x]

        assert found != []

    # The Exception function logs with custom dimensions.
    def test_exception_logs_with_custom_dimensions(self):
        self.__set_connection_string()
        logger = azure_logger.AzureLogger(handlers=[self.__mock_log_handler])
        exception = KeyError("KeyError")

        logger.exception(exception, message="message", **{"property": "property_value"})

        found = [
            x
            for x in self.__mock_log_handler.get_records()
            if "property: property_value" in x
        ]

        assert found != []

    # The Exception function logs when correlation id is set.
    def test_exception_logs_when_correlation_id_is_set(self):
        self.__set_connection_string()
        correlation_id = uuid.uuid4()

        logger = azure_logger.AzureLogger(
            correlation_id=correlation_id,
            handlers=[self.__mock_log_handler],
        )

        exception = KeyError("KeyError")

        logger.exception(exception, message="message")

        found = [x for x in self.__mock_log_handler.get_records() if "message" in x]

        assert found != []
        assert len(found) == 1
        assert correlation_id.hex in found[0]

    # The Log function should log the message to debug
    def test_log_logs_message_as_debug(self):
        self.__set_connection_string()
        self.__mock_log_handler.setLevel(logging.DEBUG)

        logger = azure_logger.AzureLogger(
            level=logging.DEBUG,
            handlers=[self.__mock_log_handler],
        )

        log_message = "some log message"

        logger.log(log_message)

        found = [x for x in self.__mock_log_handler.get_records() if log_message in x]

        assert found != []

    # The Log function should log message and customer properties to debug
    def test_log_logs_message_and_custom_properties_as_debug(self):
        self.__set_connection_string()
        self.__mock_log_handler.setLevel(logging.DEBUG)

        logger = azure_logger.AzureLogger(
            level=logging.DEBUG,
            handlers=[self.__mock_log_handler],
        )

        log_message = "some log message"

        logger.log(log_message, **{"property": "property_value"})

        found = [
            x
            for x in self.__mock_log_handler.get_records()
            if "property: property_value" in x
        ]

        assert found != []

    # The Log function should log the message to debug when correlation id is set
    def test_log_logs_message_as_debug_when_correlation_id_is_set(self):
        self.__set_connection_string()

        correlation_id = uuid.uuid4()

        self.__mock_log_handler.setLevel(logging.DEBUG)

        logger = azure_logger.AzureLogger(
            level=logging.DEBUG,
            correlation_id=correlation_id,
            handlers=[self.__mock_log_handler],
        )

        log_message = "some log message"

        logger.log(log_message)

        found = [x for x in self.__mock_log_handler.get_records() if log_message in x]

        assert found != []
        assert correlation_id.hex in found[0]

    def test_measure_float_records_the_value(self) -> None:
        self.__set_connection_string()

        correlation_id = uuid.uuid4()

        self.__mock_log_handler.setLevel(logging.DEBUG)

        logger = azure_logger.AzureLogger(
            level=logging.DEBUG,
            correlation_id=correlation_id,
            handlers=[self.__mock_log_handler],
        )

        measure = logger.create_measure_float(
            "name",
            "description",
            "m",
            azure_logger.AzureLogger.MetricAggregationType.CountAggregation,
        )

        logger.measure_float(measure, 12.34)

        assert logger._mmap.measurement_map[measure] == 12.34

    def test_measure_int_records_the_value(self) -> None:
        self.__set_connection_string()

        correlation_id = uuid.uuid4()

        self.__mock_log_handler.setLevel(logging.DEBUG)

        logger = azure_logger.AzureLogger(
            level=logging.DEBUG,
            correlation_id=correlation_id,
            handlers=[self.__mock_log_handler],
        )

        measure = logger.create_measure_int(
            "name",
            "description",
            "m",
            azure_logger.AzureLogger.MetricAggregationType.CountAggregation,
        )

        logger.measure_int(measure, 1234)

        assert logger._mmap.measurement_map[measure] == 1234
