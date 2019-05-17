# Bedrock Airflow Plugin

This folder contains Bedrock-specific plugin, to be used with Airflow.

## Setup

1. Copy `bedrock_plugin.py` to `airflow/plugins/` (here `airflow` is your Airflow folder)
2. Create a connection:
   1. Open the Airflow web server, by `airflow webserver`
   2. Go to Admin > Connections
   3. Click on "Create" at the top
   4. Fill in required authentication details using one of the schemes below:

### OAuth

```
Conn Id: anything, to be used with your DAGs, e.g. "bedrock"
Conn Type: HTTP
Host: api.bdrk.ai
Schema: https
Extra: a JSON object containing `client_id` and `client_secret` (received from Auth0),
       e.g. {"client_id": "some_client_id", "client_secret": "some_client_secret"}
```

### Basic Auth

```
Conn Id: anything, to be used with your DAGs, e.g. "bedrock"
Conn Type: HTTP
Host: api.bdrk.ai
Schema: https
Login: username provisioned by basis, e.g. "user@bdrk.ai"
Password: password provisioned by basis, e.g. "password"
Extra: must be left empty
```
