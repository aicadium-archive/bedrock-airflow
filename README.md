# Bedrock Airflow DAG

This repository contains example Airflow DAGs for running pipelines on Bedrock.

## Instructions

1. Generate an API token on Bedrock
2. Copy `examples/bedrock_dag_v2.py` to `~/airflow/dags/`, where `~/airflow` is path to your Airflow folder
3. Open the Airflow web server by running `airflow webserver`
4. Create a connection:
   1. Go to Admin > Connections
   2. Click on "Create" at the top
   3. Fill in the connection details like below, leaving all other fields empty

```
Conn Id: bedrock
Conn Type: HTTP
Host: api.bdrk.ai
Schema: https
Extra: {"X-Bedrock-Access-Token": "<your generated here>"}
```

5. Switch on `bedrock_dag_v2` DAG from Airflow UI or run it from the command line, for e.g.

```bash
$> airflow test bedrock_dag_v2 create 2019-05-21
```

## Advanced Usage

You can customise `bedrock_dag_v2.py` to fit your own Bedrock workflow. Generally, this requires creating one `HttpOperator` for each Bedrock API call and chaining them together in the same DAG. The list of available APIs is documented in our [tutorial wiki](https://github.com/basisai/bedrock-airflow/wiki/Interacting-with-Bedrock-API).

# Bedrock Airflow Plugin (Deprecated)

[![Build Status](https://travis-ci.com/basisai/bedrock-airflow.svg?branch=master)](https://travis-ci.com/basisai/bedrock-airflow)

This folder contains an Airflow plugin for training ML models on a schedule using the Bedrock platform.

## Setup

1. Generate an API token on Bedrock UI
2. Copy `bedrock_plugin.py` to `airflow/plugins/`
   1. For self-hosted airflow, the destination is in your home directory
   2. For Google Cloud Composer, the `airflow` directory points to a GCS bucket
3. Create a connection:
   1. Open the Airflow web server
   2. Go to Admin > Connections
   3. Click on "Create" at the top
   4. Fill in the connection details like below, leaving all other fields empty

 ```
Conn Id: bedrock
Conn Type: HTTP
Host: api.bdrk.ai
Schema: https
Extra: {"X-Bedrock-Access-Token": "<your generated here>"}
```

## Run a DAG

We recommend creating a new DAG for each training pipeline so that each model can be trained on its own schedule. To get started, create your first training pipeline on Bedrock UI and take note of its public id (the last portion of the pipeline's URL).

1. Copy `examples/bedrock_dag.py` to `airflow/dags/`
2. Create variables for `RunPipelineOperator` to match your new pipeline

```
{
  "pipeline_public_id": "churn-prediction-123456",
  "environment_public_id": "<obtained from dropdown list in run pipeline page>"
}
```

3. Run `bedrock_dag` from Airflow UI or from the command line

```bash
$> airflow test bedrock_dag create 2019-05-21
```

4. [Optional] Login to https://bedrock.basis-ai.com to verify that your training pipeline run has completed
