from os import getenv

from airflow import DAG
from airflow.models import Variable
from airflow.operators.bedrock_plugin import CreatePipelineOperator, RunPipelineOperator
from airflow.utils.dates import days_ago

dag = DAG("bedrock_dag", start_date=days_ago(1), catchup=False)

create_options = Variable.get(
    "bedrock_create_training_pipeline_config",
    deserialize_json=True,
    default_var={
        "uri": getenv("GIT_REPO_URI", "bedrock"),
        "ref": getenv("GIT_REPO_REF", "master"),
        "username": getenv("GIT_REPO_USERNAME", "bedrock"),
        "password": getenv("GIT_REPO_PASSWORD", "blahblah"),
        "config_file_path": getenv("GIT_REPO_CONFIG_FILE_PATH", "bedrock.hcl"),
    },
)

create = CreatePipelineOperator(
    task_id="create_pipeline",
    dag=dag,
    conn_id="bedrock",
    name="My Bedrock Airflow DAG",
    **create_options
)

run = RunPipelineOperator(
    task_id="run_pipeline",
    dag=dag,
    conn_id="bedrock",
    pipeline_id="{{ ti.xcom_pull(task_ids='create_pipeline') }}",
)  # this field is templated, can pull xcom from previous operators

create >> run
