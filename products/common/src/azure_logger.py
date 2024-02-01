import logging
import logging.handlers
import os
import sys
import uuid
from enum import Enum
from typing import List

from exporter_connection_string import get_exporter_connection_string
from extras_formatter import ExtrasFormatter
from opencensus.ext.azure import metrics_exporter
from opencensus.ext.azure.log_exporter import AzureEventHandler, AzureLogHandler
from opencensus.ext.azure.trace_exporter import AzureExporter
from opencensus.stats import aggregation as aggregation_module
from opencensus.stats import measure as measure_module
from opencensus.stats import stats as stats_module
from opencensus.stats import view as view_module
from opencensus.tags import tag_map as tag_map_module
from opencensus.trace import config_integration, span_context
from opencensus.trace.samplers import AlwaysOnSampler
from opencensus.trace.tracer import Tracer

stats = stats_module.stats
view_manager = stats.view_manager
stats_recorder = stats.stats_recorder


class AzureLogger:
    _exporter = metrics_exporter.new_metrics_exporter(
        connection_string=get_exporter_connection_string()
    )
    view_manager.register_exporter(_exporter)
    _mmap = stats_recorder.new_measurement_map()
    _tag_map = tag_map_module.TagMap()

    class MetricAggregationType(Enum):
        CountAggregation = 1
        DistributionAggregation = 2
        LastValueAggregation = 3

    """
    AzureLogger class used to send telemetry (events, logs, metrics and exceptions)
    to Application Insights in Azure.
    """

    def __init__(
        self,
        handlers: List[logging.Handler] = list(),
        correlation_id: uuid.UUID = uuid.UUID(int=0),
        level: int = logging.INFO,
    ):
        """
        Creates an instance of an Azure_Logger object.

        Parameters
        ----------
        self: Azure_Logger

            Required self parameter.
        name: string
            Name of the logger.
        handlers: List[logging.Handler]
            List of additional handlers to add if required.
        correlation_Id: string
            Optional correlation id used to tie multiple logger calls together
            in Azure Monitor.
        level: int
            Logging level to use, defaults to Info if not provided.
        """

        self.__correlation_id = correlation_id
        self.__level = level

        # Adding the stdout handler so we log to console.
        stdout_handler = logging.StreamHandler(stream=sys.stdout)
        stdout_handler.name = "stdout"
        handlers.append(stdout_handler)

        try:
            self.__connection_string = os.environ["APPLICATIONINSIGHTS_CONNECTION_STRING"]

            if self.__use_app_insights():
                trace_handler = AzureLogHandler(connection_string=self.__connection_string)
                trace_handler.name = __name__ + ".AzureLogHandler"

                event_handler = AzureEventHandler(
                    connection_string=self.__connection_string
                )
                event_handler.name = __name__ + ".AzureEventHandler"

                handlers.append(trace_handler)
                handlers.append(event_handler)

            if self.__has_correlation_id():
                if self._tag_map.tag_key_exists("correlation_id") is False:
                    self._tag_map.insert("correlation_id", self.__correlation_id.hex)

                config_integration.trace_integrations(["logging"])
                trace_context = span_context.SpanContext(trace_id=self.__correlation_id.hex)
                event_context = span_context.SpanContext(trace_id=self.__correlation_id.hex)
                self.__trace_tracer = Tracer(
                    span_context=trace_context,
                    sampler=AlwaysOnSampler(),
                    exporter=AzureExporter(
                        connection_string=get_exporter_connection_string()
                    ),
                )
                self.__event_tracer = Tracer(
                    span_context=event_context,
                    sampler=AlwaysOnSampler(),
                    exporter=AzureExporter(
                        connection_string=get_exporter_connection_string()
                    ),
                )

            self.__trace_logger = logging.getLogger(__name__ + ".trace")
            self.__event_logger = logging.getLogger(__name__ + ".events")

            self.__trace_logger.handlers.clear()
            self.__event_logger.handlers.clear()

            self.__add_log_handlers(handlers=handlers)
            self.__trace_logger.info(
                "App Insights logging configured successfully. "
                "Logs will be exported to App Insights."
            )
        except KeyError:
            self.__trace_logger = logging.getLogger(__name__ + ".trace")
            self.__event_logger = logging.getLogger(__name__ + ".events")
            self.__connection_string = ""
            self.__add_log_handlers(handlers=handlers)
            self.__trace_logger.warning(
                "Environment variable APPLICATIONINSIGHTS_CONNECTION_STRING not set "
                "or is invalid, local logging only."
            )

    def create_measure_float(
        self,
        name: str,
        description: str,
        unit: str,
        aggregation_type: MetricAggregationType,
    ) -> measure_module.MeasureFloat:
        """
        Returns a MeasureFloat instance.

        Parameters
        ----------
            name: string
                Name of the measure to create.
            description: string
                Description of the measure.
            unit: string
                Unit for the measure. MUST follow Unified code for units of measure.

        Returns
        -------
            MeasureFloat instance.
        """
        measure = measure_module.MeasureFloat(name, description, unit)

        aggregation = None
        if aggregation_type == AzureLogger.MetricAggregationType.CountAggregation:
            aggregation = aggregation_module.CountAggregation()
        elif aggregation_type == AzureLogger.MetricAggregationType.DistributionAggregation:
            aggregation = aggregation_module.DistributionAggregation()
        else:
            aggregation = aggregation_module.LastValueAggregation()

        view = view_module.View(
            name=name,
            description=description,
            columns=["correlation_id"],
            measure=measure,
            aggregation=aggregation,
        )

        view_manager.register_view(view)
        return measure

    def measure_float(self, measure: measure_module.MeasureFloat, value: float):
        self._mmap.measure_float_put(measure=measure, value=value)
        self._mmap.record(self._tag_map)

    def create_measure_int(
        self,
        name: str,
        description: str,
        unit: str,
        aggregation_type: MetricAggregationType,
    ) -> measure_module.MeasureInt:
        """
        Returns a MeasureInt instance.

        Parameters
        ----------
            name: string
                Name of the measure to create.
            description: string
                Description of the measure.
            unit: string
                Unit for the measure. MUST follow Unified code for units of measure.

        Returns
        -------
            MeasureInt instance.
        """
        measure = measure_module.MeasureInt(name, description, unit)

        aggregation = None
        if aggregation_type == AzureLogger.MetricAggregationType.CountAggregation:
            aggregation = aggregation_module.CountAggregation()
        elif aggregation_type == AzureLogger.MetricAggregationType.DistributionAggregation:
            aggregation = aggregation_module.DistributionAggregation()
        else:
            aggregation = aggregation_module.LastValueAggregation()

        view = view_module.View(
            name=name,
            description=description,
            columns=["correlation_id"],
            measure=measure,
            aggregation=aggregation,
        )

        view_manager.register_view(view)
        return measure

    def measure_int(self, measure: measure_module.MeasureInt, value: int):
        self._mmap.measure_int_put(measure=measure, value=value)
        self._mmap.record(self._tag_map)

    def get_correlation_id(self) -> uuid.UUID:
        """
        Get property for the correlation id.

        Returns
        -------
        Correlation Id held by the class.
        """
        return self.__correlation_id

    def event(self, event_name: str, **custom_dimensions: str) -> None:
        """
        Writes the event_name to the information log stream.

        Parameters
        ----------
        self: Azure_Logger
            Required self parameter.
        event_name: string
            Name of the event, will be written to the information stream.
        custom_dimensions: dict[str,str]
            Custom properties/dimensions to include with the event.
        """
        properties = {"custom_dimensions": custom_dimensions}
        if self.__has_correlation_id() and self.__use_app_insights():
            with self.__event_tracer.span(str(self.__correlation_id)):
                self.__event_logger.info(event_name, extra=properties)
        else:
            self.__event_logger.info(event_name, extra=properties)

    def exception(
        self, exception: BaseException, message: str = "", **custom_dimensions: str
    ) -> None:
        """
        Writes the exception and message to the exception log stream.

        Parameters
        ----------
        self: Azure_Logger
            Required self parameter.
        exception: BaseException
            Exception object to be logged.
        message: string
            Optional message to be logged along with the exception.
        custom_dimensions: dict[str,str]
            Custom properties/dimensions to include with the exception.
        """
        properties = {"custom_dimensions": custom_dimensions}

        if self.__has_correlation_id() and self.__use_app_insights():
            with self.__trace_tracer.span(name=str(self.__correlation_id)):
                self.__trace_logger.exception(message, exc_info=exception, extra=properties)
        else:
            self.__trace_logger.exception(message, exc_info=exception, extra=properties)

    def log(self, message: str, **custom_dimensions: str) -> None:
        """
        Writes the message to the Debug log stream.

        Parameters
        ----------
        self: Azure_Logger
            Required self parameter.
        message: string
            Message to be logged to the debug stream.
        custom_dimensions: dict[str,str]
            Custom properties/dimensions to include with the log.
        """

        properties = {"custom_dimensions": custom_dimensions}
        if self.__has_correlation_id() and self.__use_app_insights():
            with self.__trace_tracer.span(name=str(self.__correlation_id)):
                self.__trace_logger.debug(message, extra=properties)
        else:
            self.__trace_logger.debug(message, extra=properties)

    def __add_log_handlers(self, handlers: List[logging.Handler] = list()):
        """
        'Private' method for setting the formatter on log streams. If the correlation id
        value is set, then it should use it in the formatter.

        Parameters:
        -----------
        self: Azure_Logger
            Required self parameter.
        handlers: List[logging.Handler]
            The list of handlers to set the formatting on.
        """

        non_correlated_format = "[%(levelname)s] %(asctime)s %(message)s"
        correlated_format = "%(message)s"

        for handler in handlers:
            if handler.name == "stdout":
                handler.setFormatter(ExtrasFormatter(fmt=non_correlated_format))
            elif self.__use_app_insights():
                if self.__has_correlation_id():
                    handler.setFormatter(logging.Formatter(correlated_format))
                else:
                    handler.setFormatter(ExtrasFormatter(fmt=non_correlated_format))
            else:
                handler.setFormatter(ExtrasFormatter(fmt=non_correlated_format))
            handler.level = self.__level

            # This convoluted dance is required to keep both tests and prod code happy.
            # Tests need to make sure that each test instance has exactly one independent
            # handler per type or they fail. Prod code just doesn't want duplicates which
            # can occur when multiple loggers are in scope at the same time.
            if isinstance(handler, AzureEventHandler):
                found = [h for h in self.__event_logger.handlers if h.name == handler.name]
                if found != []:
                    self.__event_logger.handlers.remove(found[0])
                self.__event_logger.addHandler(handler)
            elif isinstance(handler, AzureLogHandler):
                found = [h for h in self.__trace_logger.handlers if h.name == handler.name]
                if found != []:
                    self.__trace_logger.handlers.remove(found[0])
                self.__trace_logger.addHandler(handler)
            else:
                found = [h for h in self.__trace_logger.handlers if h.name == handler.name]
                if found != []:
                    self.__trace_logger.handlers.remove(found[0])
                self.__trace_logger.addHandler(handler)

                found = [h for h in self.__event_logger.handlers if h.name == handler.name]
                if found != []:
                    self.__event_logger.handlers.remove(found[0])
                self.__event_logger.addHandler(handler)

        self.__trace_logger.level = self.__level
        self.__event_logger.level = self.__level

    def __has_correlation_id(self):
        return self.__correlation_id != uuid.UUID(int=0)

    def __use_app_insights(self):
        return self.__connection_string != ""
