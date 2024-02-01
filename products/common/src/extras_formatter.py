import logging


class ExtrasFormatter(logging.Formatter):
    def_keys = [
        "name",
        "msg",
        "args",
        "levelname",
        "levelno",
        "pathname",
        "filename",
        "module",
        "exc_info",
        "exc_text",
        "stack_info",
        "lineno",
        "funcName",
        "created",
        "msecs",
        "relativeCreated",
        "thread",
        "threadName",
        "processName",
        "process",
        "message",
    ]

    def format(self, record):
        string = super().format(record)
        custom_dimensions_record = {
            k: v for k, v in record.__dict__.items() if k == "custom_dimensions"
        }
        if len(custom_dimensions_record) > 0:
            for custom_dimensions in custom_dimensions_record.values():
                if str(custom_dimensions) != "{}":
                    string += " : " + str(custom_dimensions)
        return string
