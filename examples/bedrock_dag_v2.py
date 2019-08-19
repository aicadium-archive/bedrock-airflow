import json
from datetime import datetime, timedelta
from os import getenv

from airflow import DAG
from airflow.models import Variable
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.sensors.http_sensor import HttpSensor
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule

API_VERSION = "v1"
CONN_ID = "bedrock"

run_options = Variable.get(
    "bedrock_config_v2",
    deserialize_json=True,
    default_var={
        "pipeline_public_id": getenv("PIPELINE_PUBLIC_ID", "bedrock"),
        "environment_public_id": getenv("ENVIRONMENT_PUBLIC_ID", "bedrock"),
    },
)

HEADERS = {"Content-Type": "application/json"}


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
            {"environment_public_id": run_options["environment_public_id"]}
        ),
        headers=HEADERS,
        response_check=lambda response: response.status_code == 202,
        xcom_push=True,
    )

    retry_timeout = timedelta(hours=12)

    def is_success(response, **kwargs):
        status = response.json()["status"]
        if status == "Succeeded":
            return True
        if status in ["Failed", "Stopped"]:
            raise Exception("Pipeline run failed: {}".format(response))
        if (
            # Context is not available in airflow v1.10.4
            "dag_run" in kwargs
            and datetime.utcnow() > kwargs["dag_run"].start_date + retry_timeout
        ):
            raise Exception("Exceeded retry timeout: {}".format(retry_timeout))
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
        timeout=retry_timeout.total_seconds(),
        provide_context=True,
        # Sensor will fail immediately on non-200 response code, retry a few times
        retries=5,
        retry_delay=timedelta(seconds=60),
    )

    stop_run = JsonHttpOperator(
        task_id="stop_run",
        http_conn_id=CONN_ID,
        endpoint="{}/training_run/{}/status".format(
            API_VERSION, "{{ ti.xcom_pull(task_ids='run_pipeline')['entity_id'] }}"
        ),
        method="PUT",
        headers=HEADERS,
        response_check=lambda response: response.status_code == 200,
        trigger_rule=TriggerRule.ONE_FAILED,
        xcom_push=True,
    )

    run_pipeline >> check_status >> stop_run
