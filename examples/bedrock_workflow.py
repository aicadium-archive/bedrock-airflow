from datetime import timedelta

from airflow import DAG
from airflow.operators.subdag_operator import SubDagOperator
from airflow.utils.dates import days_ago

from bedrock_common import deploy_subdag, train_subdag, undeploy_subdag

args = {
    "owner": "airflow",
    "depends_on_past": False,
    "wait_for_downstream": False,
    "start_date": days_ago(1),
}

DAG_NAME = "bedrock_workflow"

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
        retries=3,
        retry_delay=timedelta(seconds=5),
    )

    deploy = SubDagOperator(
        task_id="deploy",
        subdag=deploy_subdag(DAG_NAME),
        retries=3,
        retry_delay=timedelta(seconds=5),
    )

    undeploy_previous = SubDagOperator(
        task_id="undeploy",
        subdag=undeploy_subdag(DAG_NAME),
        retries=3,
        retry_delay=timedelta(seconds=5),
    )

    train >> deploy >> undeploy_previous
