import os

from azure.identity import ManagedIdentityCredential
from azure.keyvault.secrets import SecretClient

KV_SECRETS_TO_READ = [
    "APPLICATIONINSIGHTS-CONNECTION-STRING",
    "PC-SDK-SUBSCRIPTION-KEY",
    "SQL-DATABASE-ODBC-CONNECTION-STRING",
]


def get_environment_from_kv(key_vault_name: str):
    """
    Get secrets from Key Vault and sets them as environment variables.

    Parameters
    ----------
    key_vault_name: str
        Key Vault DNS name.
    """
    kv_uri = f"https://{key_vault_name}.vault.azure.net"

    client_id = os.environ.get("DEFAULT_IDENTITY_CLIENT_ID")
    credential = ManagedIdentityCredential(client_id=client_id)

    client = SecretClient(vault_url=kv_uri, credential=credential)

    # Set all retrieved secrets as environment variables
    for secret in KV_SECRETS_TO_READ:
        retrieved_secret = client.get_secret(secret)
        if retrieved_secret.value is not None:
            # Need to replace hyphen with underscore in name
            os.environ[secret.replace("-", "_")] = retrieved_secret.value
