from unittest import TestCase
from unittest.mock import MagicMock, patch

from utils.key_vault_utils import KV_SECRETS_TO_READ, get_environment_from_kv


class TestKeyVaultUtils(TestCase):
    @patch("utils.key_vault_utils.SecretClient")
    @patch("os.environ")
    def test_get_environment_from_kv(self, mock_environ, mock_secret_client):
        # Mock secret value
        mock_secret_value = MagicMock()
        mock_secret_value.value = "foo"
        # Mock call to get_secret, returns secret value
        mock_get_secret = MagicMock()
        mock_get_secret.get_secret.return_value = mock_secret_value
        # Call to constructor returns mock
        mock_secret_client.return_value = mock_get_secret

        get_environment_from_kv("fakekv")

        mock_secret_client.assert_called_once()
        mock_get_secret.get_secret.assert_called()

        # Check os.environ has been updated
        for k in KV_SECRETS_TO_READ:
            mock_environ.__setitem__.assert_any_call(k.replace("-", "_"), "foo")
