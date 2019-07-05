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

## Run a DAG

1. Copy `examples/bedrock_dag.py` to `airflow/dags/` (here `airflow` is your Airflow folder)
2. Update GitHub credentials passed to `CreatePipelineOperator` to match your own repository, including

```
uri: link to repository on GitHub, e.g. "https://github.com/org/example.git"
ref: branch name or commit id, e.g. "master"
username: username if repository is private (Optional)
password: password or access token for the private repository (Optional)
```

3. Run `bedrock_dag` from airflow UI or individual tasks from the command line, for e.g.

```bash
$> airflow test bedrock_dag create 2019-05-21
```

4. [Optional] Login to https://bedrock.basis-ai.com to verify that the training pipeline has been created

## Developing

To start developing your own airflow operators for `bedrock_plugin`, you will need to install the required development dependencies.
We recommend doing it in a virtual environment to avoid potential dependency conflicts. For example,

```bash
$ python3 -m venv venv
$ source venv/bin/activate
$ pip install -r requirements-dev.txt
```

### Running unit tests

Once your local development environment is setup, you can verify that everything is working by executing our unit tests with `pytest`.

```bash
$ pytest -v tests
```
