import json
from datetime import timedelta
from os import getenv

from airflow import DAG
from airflow.models import Variable
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.sensors.http_sensor import HttpSensor
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule

API_VERSION = "v1"
# An airflow connection must be created with a valid auth token
CONN_ID = "bedrock_v2"

run_options = Variable.get(
    "bedrock_config_v2",
    deserialize_json=True,
    default_var={
        # Value can be obtained from creating a pipeline on Bedrock UI
        "pipeline_public_id": getenv("PIPELINE_PUBLIC_ID", "bedrock"),
        # Value can be obtained from environment dropdown list of run pipeline page
        "environment_public_id": getenv("ENVIRONMENT_PUBLIC_ID", "bedrock"),
    },
)


class JsonHttpOperator(SimpleHttpOperator):
    def execute(self, context):
        text = super(JsonHttpOperator, self).execute(context)
        return json.loads(text)


with DAG(dag_id="bedrock_dag_v2", start_date=days_ago(1), catchup=False) as dag:
    run_pipeline = JsonHttpOperator(
        task_id="run_pipeline",
        http_conn_id=CONN_ID,
        endpoint="{}/training_pipeline/{}/run/".format(
            API_VERSION, run_options["pipeline_public_id"]
        ),
        method="POST",
        data=json.dumps(
            {
                "environment_public_id": run_options["environment_public_id"],
                # Specifies the branch or commit for the pipeline run
                "run_source_commit": "master",
            }
        ),
        response_check=lambda response: response.status_code == 202,
        xcom_push=True,
    )

    def is_success(response):
        if response.status_code != 200:
            return False
        status = response.json()["status"]
        if status == "Succeeded":
            return True
        if status in ["Failed", "Stopped"]:
            # Exceptions will be bubbled up to stop the sensor
            raise Exception("Pipeline run failed: {}".format(response.text))
        return False

    check_status = HttpSensor(
        task_id="check_status",
        http_conn_id=CONN_ID,
        endpoint="{}/training_run/{}".format(
            API_VERSION, "{{ ti.xcom_pull(task_ids='run_pipeline')['entity_id'] }}"
        ),
        method="GET",
        response_check=is_success,
        poke_interval=60,
        # Use reschedule mode to not block the worker queue
        mode="reschedule",
        # Retry timeout should match the expected training time for this pipeline
        timeout=timedelta(hours=12).total_seconds(),
        # Avoid raising exceptions on non 2XX or 3XX status codes
        extra_options={"check_response": False},
    )

    stop_run = SimpleHttpOperator(
        task_id="stop_run",
        http_conn_id=CONN_ID,
        endpoint="{}/training_run/{}/status".format(
            API_VERSION, "{{ ti.xcom_pull(task_ids='run_pipeline')['entity_id'] }}"
        ),
        method="PUT",
        response_check=lambda response: response.status_code == 202,
        trigger_rule=TriggerRule.ONE_FAILED,
        xcom_push=True,
    )

    run_pipeline >> check_status >> stop_run
