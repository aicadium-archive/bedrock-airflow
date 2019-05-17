from datetime import datetime

from airflow import DAG
from airflow.operators.bedrock_plugin import CreatePipelineOperator, RunPipelineOperator

dag = DAG("bedrock_dag", start_date=datetime.now())

create = CreatePipelineOperator(
    task_id="create_pipeline",
    dag=dag,
    conn_id="bedrock",
    name="Airflow-created pipeline",
    uri="https://github.com/org/example.git",
    ref="master",
    username="bedrock",
    password="blahblah",
)

run = RunPipelineOperator(
    task_id="run_pipeline",
    dag=dag,
    conn_id="bedrock",
    pipeline_id="{{ ti.xcom_pull(task_ids='create_pipeline') }}",
)  # this field is templated, can pull xcom from previous operators

create >> run
