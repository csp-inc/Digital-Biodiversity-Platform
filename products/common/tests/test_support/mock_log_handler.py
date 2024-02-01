import logging


class MockLogHandler(logging.Handler):
    """
    MockLogHandler used to intercept log messages for testing
    purposes only.
    """

    def __init__(self, level: int):
        super().__init__(level=level)
        self.__records = []

    def get_records(self):
        """
        Returns the list of records held by the class.

        Parameters
        ----------
        self: Azure_Logger
            Required self parameter.

        Returns
        -------
            __records list.

        """
        return self.__records

    def clear_records(self):
        """
        Clears the records list held by the class.
        Used to clean up after each test.

        Parameters
        ----------
        self: Azure_Logger
            Required self parameter.
        """
        self.__records.clear()

    def emit(self, record):
        """
        Called by the Python Logging framework when an item
        is to be logged.

        Parameters
        ----------
        self: Azure_Logger
            Required self parameter.

        record: LogRecord
            Item to be logged.

        """
        formatted_message = self.format(record)
        # In the real Azure Handler, the traceId does not appear in
        # the message text, but we need to do this here to allow it
        # to be tested.

        if hasattr(record, "traceId"):
            self.get_records().append(record.traceId + " " + formatted_message)
        else:
            self.get_records().append(formatted_message)

        if hasattr(record, "custom_dimensions"):
            for name, value in record.custom_dimensions.items():
                self.get_records().append(name + ": " + value)
