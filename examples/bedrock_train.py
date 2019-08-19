import json
from os import getenv

from airflow import DAG
from airflow.models import Variable
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.sensors.http_sensor import HttpSensor
from airflow.utils.dates import days_ago

API_VERSION = "v1"
CONN_ID = "bedrock"

run_options = Variable.get(
    "bedrock_config",
    deserialize_json=True,
    default_var={"pipeline_public_id": getenv("PIPELINE_PUBLIC_ID", "bedrock")},
)

HEADERS = {"Content-Type": "application/json"}


class JsonHttpOperator(SimpleHttpOperator):
    def execute(self, context):
        text = super(JsonHttpOperator, self).execute(context)
        return json.loads(text)


def train_subdag(parent_dag_name):
    with DAG(
        dag_id="{}.bedrock_train".format(parent_dag_name), start_date=days_ago(1)
    ) as dag:
        get_environment = JsonHttpOperator(
            task_id="get_environment",
            http_conn_id=CONN_ID,
            endpoint="{}/environment/".format(API_VERSION),
            method="GET",
            headers=HEADERS,
            response_check=lambda response: len(response.json()) > 0,
            xcom_push=True,
        )

        run_pipeline = JsonHttpOperator(
            task_id="run_pipeline",
            http_conn_id=CONN_ID,
            endpoint="{}/training_pipeline/{}/run/".format(
                API_VERSION, run_options["pipeline_public_id"]
            ),
            method="POST",
            data=json.dumps(
                {
                    "environment_public_id": "{{ ti.xcom_pull(task_ids='get_environment')[0]['public_id'] }}",
                    "run_source_commit": "master",
                }
            ),
            headers=HEADERS,
            response_check=lambda response: response.status_code == 202,
            xcom_push=True,
        )

        def is_success(response):
            status = response.json()["status"]
            if status == "Succeeded":
                return True
            if status in ["Failed", "Stopped"]:
                raise Exception("Pipeline run failed: {}".format(response))
            return False

        check_status = HttpSensor(
            task_id="check_status",
            http_conn_id=CONN_ID,
            endpoint="{}/training_run/{}".format(
                API_VERSION, "{{ ti.xcom_pull(task_ids='run_pipeline')['entity_id'] }}"
            ),
            method="GET",
            response_check=is_success,
            poke_interval=10,
        )

        get_environment >> run_pipeline >> check_status
        return dag
