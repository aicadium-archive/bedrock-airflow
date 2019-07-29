import sys
import pytest
from airflow.hooks.http_hook import HttpHook
from airflow.models import Connection

if sys.version_info >= (3, 3):
    from unittest.mock import patch
else:
    from mock import patch


def get_connection_mock(conn_id):
    conn = Connection(
        conn_id=conn_id,
        conn_type="HTTP",
        host="some_host",
        extra='{"client_id": "some_client", "client_secret": "some_secret"}',
    )
    return conn


@pytest.fixture
def airflow_connection():
    with patch.object(HttpHook, "get_connection", side_effect=get_connection_mock):
        yield "span_test"
