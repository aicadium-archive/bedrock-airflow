import json
import time
from builtins import super

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

    environment_id: str
        The public id of the environment to run the pipeline in. This value
        can be obtained from Bedrock UI by clicking on run pipeline.

    status_poke_interval : int
        Interval to check the status of the pipeline
    """

    RUN_PIPELINE_PATH = "/{}/pipeline/{{}}/run/".format(API_VERSION)
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
        status_poke_interval=15,
        **kwargs
    ):
        super().__init__(**kwargs)
        self.conn_id = conn_id
        self.pipeline_id = pipeline_id
        self.pipeline_run_id = None
        self.run_source_commit = run_source_commit
        self.environment_id = environment_id
        self.status_poke_interval = status_poke_interval

    def execute(self, context):
        # Run the training pipeline
        hook = HttpHook(method="POST", http_conn_id=self.conn_id)
        data = json.dumps(
            {
                "environment_public_id": self.environment_id,
                "run_source_commit": self.run_source_commit,
            }
        )

        try:
            res = hook.run(
                RunPipelineOperator.RUN_PIPELINE_PATH.format(self.pipeline_id),
                data=data,
            )
        except AirflowException as ex:
            self.log.error("Failed to run pipeline")
            raise ex

        data = json.loads(res.content)
        pipeline_run_id = data["entity_id"]
        self.pipeline_run_id = pipeline_run_id  # Used for cleanup only
        self.log.info(
            "Pipeline successfully run, pipeline run ID: {}".format(pipeline_run_id)
        )

        # Poll pipeline run status
        get_hook = HttpHook(method="GET", http_conn_id=self.conn_id)
        while True:
            status = self._check_status(get_hook, pipeline_run_id)
            if status == "Succeeded":
                return
            if status in ["Failed", "Stopped"]:
                raise Exception("Pipeline run failed: {}".format(pipeline_run_id))
            time.sleep(self.status_poke_interval)

        self._cleanup_run(pipeline_run_id, post_hook=hook)
        raise Exception("Run timed out {}".format(pipeline_run_id))

    def _check_status(self, hook, pipeline_run_id):
        self.log.info("Checking status")

        res = hook.run(
            RunPipelineOperator.GET_PIPELINE_RUN_PATH.format(pipeline_run_id),
            # Avoid raising exceptions on non 2XX or 3XX status codes
            extra_options={"check_response": False},
        )

        if res.status_code != 200:
            return "Running"

        data = json.loads(res.content)
        status = data["status"]
        self.log.info("Status of pipeline run: {}".format(status))
        return status

    def _cleanup_run(self, pipeline_run_id, post_hook=None):
        self.log.info("Stopping pipeline run")
        hook = post_hook or HttpHook(method="POST", http_conn_id=self.conn_id)
        hook.run(
            RunPipelineOperator.STOP_PIPELINE_RUN_PATH.format(pipeline_run_id),
            extra_options={"check_response": False},
        )
        # Don't raise if we failed to stop

    def on_kill(self):
        if self.pipeline_run_id:
            self._cleanup_run(self.pipeline_run_id)


class BedrockPlugin(AirflowPlugin):
    name = "bedrock_plugin"
    operators = [RunPipelineOperator]
