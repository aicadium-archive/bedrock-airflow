from datetime import timedelta

from airflow import DAG
from airflow.models import DagBag
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.subdag_operator import SubDagOperator
from airflow.utils.dates import days_ago

from bedrock_common import (
    API_VERSION,
    CONN_ID,
    JsonHttpOperator,
    deploy_subdag,
    run_options,
    train_subdag,
)

args = {
    "owner": "airflow",
    "depends_on_past": False,
    "wait_for_downstream": False,
    "start_date": days_ago(1),
}

DAG_NAME = "bedrock_workflow"


def clear_subdag(context):
    dag_id = "{}.{}".format(context["dag"].dag_id, context["ti"].task_id)
    execution_date = context["execution_date"]
    sdag = DagBag().get_dag(dag_id)
    sdag.clear(
        start_date=execution_date,
        end_date=execution_date,
        only_failed=False,
        only_running=False,
        confirm_prompt=False,
        include_subdags=False,
    )


with DAG(
    dag_id=DAG_NAME,
    default_args=args,
    schedule_interval="@daily",
    catchup=False,
    max_active_runs=1,
    start_date=days_ago(1),
) as dag:
    train = SubDagOperator(
        task_id="train",
        subdag=train_subdag(DAG_NAME),
        retries=2,
        retry_delay=timedelta(seconds=10),
        on_retry_callback=clear_subdag,
    )

    deploy = SubDagOperator(
        task_id="deploy",
        subdag=deploy_subdag(DAG_NAME),
        retries=2,
        retry_delay=timedelta(seconds=10),
        on_retry_callback=clear_subdag,
    )

    get_endpoint = JsonHttpOperator(
        task_id="get_endpoint",
        http_conn_id=CONN_ID,
        endpoint="{}/endpoint/{}".format(
            API_VERSION, run_options["pipeline_public_id"]
        ),
        method="GET",
        response_check=lambda response: len(response.json()["deployments"]) > 0,
        xcom_push=True,
    )

    def undeploy_model(**kwargs):
        deployments = kwargs["ti"].xcom_pull(task_ids="get_endpoint")["deployments"]
        past_models = sorted(deployments, key=lambda d: d["created_at"])[:-1]
        # TODO: find a better way to track these dynamic operators
        for model in past_models:
            SimpleHttpOperator(
                task_id="undeploy_model",
                http_conn_id=CONN_ID,
                endpoint="{}/serve/id/{}/deploy".format(
                    API_VERSION, model["entity_id"]
                ),
                method="DELETE",
                response_check=lambda response: response.status_code == 202,
                retries=3,
                retry_delay=timedelta(minutes=1),
            ).execute(kwargs)

    undeploy_previous = PythonOperator(
        task_id="undeploy_previous",
        python_callable=undeploy_model,
        provide_context=True,
    )

    train >> deploy >> get_endpoint >> undeploy_previous
