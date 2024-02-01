import os


def get_exporter_connection_string() -> str:
    """
    This function is required to keep the AzureLogger's exporter
    happy during tests when there is no connection string set.
    If the env-var is set, return it (prod case) - otherwise
    return something that looks like a connection string (test case).
    """
    if os.environ.get("APPLICATIONINSIGHTS_CONNECTION_STRING") is None:
        return (
            "InstrumentationKey=00000000-0000-0000-0000-000000000000;"
            "IngestionEndpoint=https://fake.com/;"
            "LiveEndpoint=https://fake.com/"
        )
    else:
        return os.environ["APPLICATIONINSIGHTS_CONNECTION_STRING"]
