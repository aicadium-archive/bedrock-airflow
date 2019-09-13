import json
import time
from builtins import super
from datetime import datetime, timedelta

from airflow.exceptions import AirflowException
from airflow.hooks.http_hook import HttpHook
from airflow.models import BaseOperator
from airflow.plugins_manager import AirflowPlugin

API_VERSION = "v1"


class RunPipelineOperator(BaseOperator):
    """Runs a pipeline and monitors it

    Attributes
    ----------
    conn_id : str
        Connection that has the base API url

    pipeline_id : str
        Pipeline public id

    run_source_commit : str
        The git commit version to run. If the branch is specified, the
        latest commit of the branch is fetched, else if None is specified,
        the commit version when the pipeline was created will be fetched

    environment_id : str
        The public id of the environment to run the pipeline in. This value
        can be obtained from Bedrock UI by clicking on run pipeline.

    resources : dict(str)
        Resources to run the pipeline, of the form
        {"cpu": "4",
         "memory": "16G"}

    status_poke_interval : int
        Interval to check the status of the pipeline

    run_timeout : timedelta
        Expected running time of the pipeline. Used as a hard timeout to
        stop the operator.
    """

    RUN_PIPELINE_PATH = "/{}/training_pipeline/{{}}/run/".format(API_VERSION)
    GET_PIPELINE_RUN_PATH = "/{}/run/{{}}".format(API_VERSION)
    STOP_PIPELINE_RUN_PATH = "/{}/training_run/{{}}/status".format(API_VERSION)
    WAIT_STATUS = ["Running", "Queued"]
    SUCCESS_STATUS = ["Succeeded"]

    template_fields = ("pipeline_id",)

    def __init__(
        self,
        conn_id,
        pipeline_id,
        run_source_commit,  # specify branch for latest commit, e.g., 'master'
        environment_id,  # obtained from run pipeline page on Bedrock UI
        resources=None,  # specify resources
        status_poke_interval=15,
        run_timeout=timedelta(hours=1),
        **kwargs
    ):
        super().__init__(**kwargs)
        if resources is None:
            resources = {"cpu": "2", "memory": "4G"}
        self.conn_id = conn_id
        self.pipeline_id = pipeline_id
        self.pipeline_run_id = None
        self.run_source_commit = run_source_commit
        self.environment_id = environment_id
        self.resources = resources
        self.status_poke_interval = status_poke_interval
        self.run_timeout = run_timeout

    def execute(self, context):
        # Run the training pipeline
        hook = HttpHook(method="POST", http_conn_id=self.conn_id)
        data = json.dumps(
            {
                "environment_public_id": self.environment_id,
                "run_source_commit": self.run_source_commit,
                "resources": self.resources,
            }
        )

        try:
            res = hook.run(
                RunPipelineOperator.RUN_PIPELINE_PATH.format(self.pipeline_id),
                data=data,
                headers=hook.get_connection(self.conn_id).extra_dejson,
            )
        except AirflowException as ex:
            self.log.error("Failed to run pipeline")
            raise ex

        pipeline_run_id = json.loads(res.content)["entity_id"]
        self.pipeline_run_id = pipeline_run_id  # Used for cleanup only
        self.log.info(
            "Pipeline successfully run, pipeline run ID: {}".format(pipeline_run_id)
        )

        # Poll pipeline run status
        deadline = datetime.utcnow() + self.run_timeout
        get_hook = HttpHook(method="GET", http_conn_id=self.conn_id)
        while datetime.utcnow() < deadline:
            if self._check_status(get_hook, pipeline_run_id):
                return
            time.sleep(self.status_poke_interval)

        self._cleanup_run(pipeline_run_id)
        raise Exception("Run timed out {}".format(pipeline_run_id))

    def _check_status(self, hook, pipeline_run_id):
        self.log.info("Checking status")

        res = hook.run(
            RunPipelineOperator.GET_PIPELINE_RUN_PATH.format(pipeline_run_id),
            headers=hook.get_connection(self.conn_id).extra_dejson,
            # Avoid raising exceptions on non 2XX or 3XX status codes
            extra_options={"check_response": False},
        )

        if res.status_code != 200:
            self.log.warning("Non-200 status check: {}".format(res.content))
            return False

        status = json.loads(res.content)["status"]
        self.log.info("Status of pipeline run: {}".format(status))

        if status in self.SUCCESS_STATUS:
            return True
        elif status in self.WAIT_STATUS:
            return False
        else:
            raise Exception("Run status is {}".format(status))

    def _cleanup_run(self, pipeline_run_id):
        self.log.info("Stopping pipeline run")
        hook = HttpHook(method="PUT", http_conn_id=self.conn_id)
        hook.run(
            RunPipelineOperator.STOP_PIPELINE_RUN_PATH.format(pipeline_run_id),
            headers=hook.get_connection(self.conn_id).extra_dejson,
            extra_options={"check_response": False},
        )
        # Don't raise if we failed to stop

    def on_kill(self):
        if self.pipeline_run_id:
            self._cleanup_run(self.pipeline_run_id)


class BedrockPlugin(AirflowPlugin):
    name = "bedrock_plugin"
    operators = [RunPipelineOperator]
