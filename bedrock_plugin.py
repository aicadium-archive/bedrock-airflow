#!/usr/bin/python3
import datetime
import json
import time
from base64 import b64encode
from builtins import super
from http.client import HTTPSConnection

import requests
from airflow.exceptions import AirflowException
from airflow.hooks.http_hook import HttpHook
from airflow.models import BaseOperator
from airflow.plugins_manager import AirflowPlugin

API_VERSION = "v1"


class BedrockHook(HttpHook):
    def __init__(self, method, bedrock_conn_id):
        super().__init__(method=method, http_conn_id=bedrock_conn_id)
        self.conn = self.get_connection(bedrock_conn_id)
        self.base_url = "https://{}".format(self.conn.host)

        self.bedrock_token = None
        self.bedrock_token_expiry = None

    def get_conn(self, _headers=None):
        early_expiry = datetime.datetime.now() + datetime.timedelta(minutes=10)
        if self.bedrock_token is None or self.bedrock_token_expiry < early_expiry:
            self._authenticate()

        session = requests.Session()
        session.headers.update(
            {
                "Authorization": "Bearer {}".format(self.bedrock_token),
                "Content-Type": "application/json",
            }
        )
        return session

    def _authenticate(self):
        if self.conn.extra:
            conn = HTTPSConnection("bdrk.auth0.com")
            payload = {
                "client_id": self.conn.extra_dejson.get("client_id"),
                "client_secret": self.conn.extra_dejson.get("client_secret"),
                "audience": self.base_url,
                "grant_type": "client_credentials",
            }
            headers = {"Content-Type": "application/json"}
            conn.request("POST", "/oauth/token", json.dumps(payload), headers)
        else:
            # TODO: Remove basic auth support once we fully support oauth
            conn = HTTPSConnection(self.conn.host)
            auth_token = b64encode(
                "{}:{}".format(self.conn.login, self.conn.password).encode()
            ).decode("ascii")
            headers = {"Authorization": "Basic {}".format(auth_token)}
            conn.request("GET", "/v1/auth/login", headers=headers)

        resp = conn.getresponse()
        data = json.loads(resp.read())

        if not data["access_token"]:
            raise Exception(data)

        self.bedrock_token = data["access_token"]
        self.bedrock_token_expiry = datetime.datetime.now() + datetime.timedelta(
            seconds=int(data["expires_in"])
        )


class CreatePipelineOperator(BaseOperator):
    CREATE_PIPELINE_PATH = "{}/pipeline/".format(API_VERSION)

    def __init__(
        self,
        conn_id,
        name,
        uri,
        ref,
        username,
        password,
        config_file_path="bedrock.hcl",
        **kwargs
    ):
        super().__init__(**kwargs)
        self.conn_id = conn_id
        self.name = name
        self.uri = uri
        self.ref = ref
        self.config_file_path = config_file_path
        self.username = username
        self.password = password

    def execute(self, context):
        hook = BedrockHook(method="POST", bedrock_conn_id=self.conn_id)
        data = json.dumps(
            {
                "name": self.name,
                "uri": self.uri,
                "ref": self.ref,
                "username": self.username,
                "password": self.password,
                "config_file_path": self.config_file_path,
            }
        )

        try:
            res = hook.run(CreatePipelineOperator.CREATE_PIPELINE_PATH, data=data)
        except AirflowException as ex:
            self.log.error("Failed to create pipeline")
            raise ex

        data = json.loads(res.content)
        public_id = data["public_id"]
        self.log.info("Pipeline successfully created, public ID: {}".format(public_id))
        return public_id


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

    status_poke_interval : int
        Interval to check the status of the pipeline
    """

    GET_ENVIRONMENT_PATH = "{}/environment/".format(API_VERSION)
    RUN_PIPELINE_PATH = "{}/pipeline/{{}}/run/".format(API_VERSION)
    GET_PIPELINE_RUN_PATH = "{}/run/{{}}".format(API_VERSION)
    STOP_PIPELINE_RUN_PATH = "{}/training_run/{{}}/status".format(API_VERSION)
    WAIT_STATUS = ["Running", "Queued"]
    SUCCESS_STATUS = ["Succeeded"]

    template_fields = ("pipeline_id",)

    def __init__(
        self,
        conn_id,
        pipeline_id,
        run_source_commit,  # specify branch for latest commit, e.g., 'master'
        status_poke_interval=15,
        **kwargs
    ):
        super().__init__(**kwargs)
        self.conn_id = conn_id
        self.pipeline_id = pipeline_id
        self.pipeline_run_id = None
        self.run_source_commit = run_source_commit
        self.status_poke_interval = status_poke_interval

    def execute(self, context):
        # Get the list of environments
        get_hook = BedrockHook(method="GET", bedrock_conn_id=self.conn_id)

        try:
            res = get_hook.run(RunPipelineOperator.GET_ENVIRONMENT_PATH)
        except AirflowException as ex:
            self.log.error("Failed to run pipeline")
            raise ex

        environment_id = json.loads(res.content)[0]["public_id"]

        # Run the training pipeline
        hook = BedrockHook(method="POST", bedrock_conn_id=self.conn_id)
        data = json.dumps(
            {
                "environment_public_id": environment_id,
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
        while True:
            status = self._check_status(get_hook, pipeline_run_id)
            if status in RunPipelineOperator.WAIT_STATUS:
                time.sleep(self.status_poke_interval)
            elif status in RunPipelineOperator.SUCCESS_STATUS:
                break
            else:
                self._cleanup_run(pipeline_run_id)
                raise Exception("Run status is {}".format(status))

    def _check_status(self, hook, pipeline_run_id):
        self.log.info("Checking status")

        try:
            res = hook.run(
                RunPipelineOperator.GET_PIPELINE_RUN_PATH.format(pipeline_run_id)
            )
        except AirflowException as ex:
            self.log.error("Failed to check pipeline run status")
            self._cleanup_run(pipeline_run_id)
            raise ex

        data = json.loads(res.content)

        status = data["status"]
        self.log.info("Status of pipeline run: {}".format(status))
        return status

    def _cleanup_run(self, pipeline_run_id):
        self.log.info("Stopping pipeline run")
        try:
            hook = BedrockHook(method="POST", bedrock_conn_id=self.conn_id)
            hook.run(RunPipelineOperator.STOP_PIPELINE_RUN_PATH.format(pipeline_run_id))
        except AirflowException as ex:
            self.log.info("Failed to stop pipeline run status: {}".format(ex))
            # Don't raise if we failed to stop

    def on_kill(self):
        if self.pipeline_run_id:
            self._cleanup_run(self.pipeline_run_id)


class BedrockPlugin(AirflowPlugin):
    name = "bedrock_plugin"
    operators = [CreatePipelineOperator, RunPipelineOperator]
    hooks = [BedrockHook]
