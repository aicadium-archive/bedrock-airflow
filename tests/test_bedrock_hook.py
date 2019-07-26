import datetime
from http.client import HTTPResponse
from unittest.mock import Mock, patch

from bedrock_plugin import BedrockHook


def test_init(airflow_connection):
    h = BedrockHook(method="POST", bedrock_conn_id=airflow_connection)
    assert h.conn.extra_dejson.get("client_id") == "some_client"
    assert h.conn.extra_dejson.get("client_secret") == "some_secret"
    assert h.bedrock_token is None
    assert h.bedrock_token_expiry is None


def test_authenticate(airflow_connection):
    access_token = "some_access_token"
    expires_in = 60
    now = datetime.datetime(2001, 1, 1, 0, 0, 0)
    authenticate_return_value = (
        f'{{"access_token":"{access_token}","expires_in":"{expires_in}"}}'
    )

    h = BedrockHook(method="POST", bedrock_conn_id=airflow_connection)

    with patch.object(
        HTTPResponse, "read", return_value=authenticate_return_value
    ), patch("bedrock_plugin.datetime") as mock_datetime:
        mock_datetime.datetime.now.return_value = now
        mock_datetime.timedelta.side_effect = datetime.timedelta

        h._authenticate()

    assert h.bedrock_token == access_token
    assert h.bedrock_token_expiry == now + datetime.timedelta(seconds=expires_in)


def test_get_conn_headers(airflow_connection):
    bedrock_token = "some_bedrock_token"

    h = BedrockHook(method="POST", bedrock_conn_id=airflow_connection)
    h.bedrock_token = bedrock_token
    h.bedrock_token_expiry = datetime.datetime.now() + datetime.timedelta(days=1)

    headers = h.get_conn(None).headers

    assert headers["Authorization"] == f"Bearer {bedrock_token}"
    assert headers["Content-Type"] == f"application/json"


def test_get_conn_calls_authenticate_with_no_token(airflow_connection):
    h = BedrockHook(method="POST", bedrock_conn_id=airflow_connection)
    h.bedrock_token = None

    auth_mock = Mock()
    h._authenticate = auth_mock

    h.get_conn(None)
    auth_mock.assert_called_once()


def test_get_conn_calls_authenticate_with_expired_token(airflow_connection):
    h = BedrockHook(method="POST", bedrock_conn_id=airflow_connection)
    h.bedrock_token_expiry = datetime.datetime.now() - datetime.timedelta(days=1)

    auth_mock = Mock()
    h._authenticate = auth_mock

    h.get_conn(None)
    auth_mock.assert_called_once()
