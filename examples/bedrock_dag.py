from os import getenv

from airflow import DAG
from airflow.models import Variable
from airflow.operators.bedrock_plugin import RunPipelineOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.dates import days_ago

dag = DAG("bedrock_dag", start_date=days_ago(1), catchup=False)

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

op = DummyOperator(task_id="dummy", dag=dag)

run = RunPipelineOperator(
    task_id="run_pipeline",
    dag=dag,
    conn_id="bedrock",
    pipeline_id=run_options["pipeline_public_id"],
    environment_id=run_options["environment_public_id"],
    run_source_commit="master",
)  # `pipeline_id` is templated, can pull xcom from previous operators

op >> run
