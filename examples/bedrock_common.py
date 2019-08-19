import json
from datetime import datetime, timedelta
from os import getenv

from airflow import DAG
from airflow.models import Variable
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.operators.python_operator import PythonOperator
from airflow.sensors.http_sensor import HttpSensor
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule

API_VERSION = "v1"
# An airflow connection must be created with a valid auth token
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
    with DAG(dag_id="{}.train".format(parent_dag_name), start_date=days_ago(1)) as dag:
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
                    # Specifies the branch or commit for the pipeline run
                    "run_source_commit": "master",
                }
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

        get_environment >> run_pipeline >> check_status >> stop_run
        return dag


def deploy_subdag(parent_dag_name):
    with DAG(
        dag_id="{}.deploy".format(parent_dag_name),
        start_date=days_ago(1),
        params={"train_dag": "{}.train".format(parent_dag_name)},
    ) as dag:
        check_auc = JsonHttpOperator(
            task_id="check_auc",
            http_conn_id=CONN_ID,
            endpoint="{}/training_run/{}".format(
                API_VERSION,
                "{{ ti.xcom_pull(dag_id=params['train_dag'], task_ids='run_pipeline')['entity_id'] }}",
            ),
            method="GET",
            headers=HEADERS,
            response_check=lambda response: response.json()["metrics"]["AUC"] > 0.92,
            xcom_push=True,
        )

        deploy_model = JsonHttpOperator(
            task_id="deploy_model",
            http_conn_id=CONN_ID,
            endpoint="{}/serve/deploy".format(API_VERSION),
            method="POST",
            data=json.dumps(
                {
                    "pipeline_run_id": "{{ ti.xcom_pull(dag_id=params['train_dag'], task_ids='run_pipeline')['entity_id'] }}",
                    "ingress_protocol": "HTTP",
                    "public_id": run_options["pipeline_public_id"],
                }
            ),
            headers=HEADERS,
            response_check=lambda response: response.status_code == 202,
            xcom_push=True,
        )

        retry_timeout = timedelta(minutes=5)

        def is_success(response, **kwargs):
            status = response.json()["status"]
            if status == "Deployed":
                return True
            if status in ["Stopped", "Failed", "Error"]:
                raise Exception("Deployment failed: {}".format(response))
            if (
                # Context is not available in airflow v1.10.4
                "dag_run" in kwargs
                and datetime.utcnow() > kwargs["dag_run"].start_date + retry_timeout
            ):
                raise Exception("Exceeded retry timeout: {}".format(retry_timeout))
            return False

        check_server = HttpSensor(
            task_id="check_server",
            http_conn_id=CONN_ID,
            endpoint="{}/serve/id/{}".format(
                API_VERSION, "{{ ti.xcom_pull(task_ids='deploy_model')['entity_id'] }}"
            ),
            method="GET",
            response_check=is_success,
            # Use a short interval since deployment should be relatively fast
            poke_interval=10,
            timeout=retry_timeout.total_seconds(),
            provide_context=True,
            # Sensor will fail immediately on non-200 response code, retry a few times
            retries=5,
            retry_delay=timedelta(seconds=60),
        )

        check_auc >> deploy_model >> check_server
        return dag


def undeploy_subdag(parent_dag_name):
    with DAG(
        dag_id="{}.undeploy".format(parent_dag_name), start_date=days_ago(1)
    ) as dag:
        get_endpoint = JsonHttpOperator(
            task_id="get_endpoint",
            http_conn_id=CONN_ID,
            endpoint="{}/endpoint/{}".format(
                API_VERSION, run_options["pipeline_public_id"]
            ),
            method="GET",
            headers=HEADERS,
            response_check=lambda response: len(response.json()["deployments"]) > 0,
            xcom_push=True,
        )

        def undeploy_previous(**kwargs):
            deployments = kwargs["ti"].xcom_pull(task_ids="get_endpoint")["deployments"]
            past_models = sorted(deployments, key=lambda d: d["created_at"])[:-1]
            for model in past_models:
                SimpleHttpOperator(
                    task_id="undeploy_model",
                    http_conn_id=CONN_ID,
                    endpoint="{}/serve/id/{}/deploy".format(
                        API_VERSION, model["entity_id"]
                    ),
                    method="DELETE",
                    headers=HEADERS,
                    response_check=lambda response: response.status_code == 202,
                ).execute(kwargs)

        undeploy_previous = PythonOperator(
            task_id="undeploy_previous",
            python_callable=undeploy_previous,
            provide_context=True,
        )

        check_endpoint = HttpSensor(
            task_id="check_endpoint",
            http_conn_id=CONN_ID,
            endpoint="{}/endpoint/{}".format(
                API_VERSION, run_options["pipeline_public_id"]
            ),
            method="GET",
            response_check=lambda response: len(response.json()["deployments"]) == 1,
            # Use a short interval since deployment should be relatively fast
            poke_interval=10,
            timeout=300,
            provide_context=True,
            # Sensor will fail immediately on non-200 response code, retry a few times
            retries=5,
            retry_delay=timedelta(seconds=60),
        )

        get_endpoint >> undeploy_previous >> check_endpoint
        return dag
