import datetime
import sys
import time
import pytest
from airflow import DAG
from bedrock_plugin import BedrockHook, RunPipelineOperator

if sys.version_info >= (3, 3):
    from unittest.mock import PropertyMock, patch
else:
    from mock import PropertyMock, patch


def test_run_pipeline(airflow_connection):
    environment_id = "test-environment"
    pipeline_id = "some-pipeline-id"
    run_id = "some-run-id"

    dag = DAG("bedrock_dag", start_date=datetime.datetime.now())
    op = RunPipelineOperator(
        task_id="run_pipeline",
        dag=dag,
        conn_id=airflow_connection,
        pipeline_id=pipeline_id,
    )

    def bedrockhook_run_side_effect(endpoint, *_args, **_kwargs):
        resp = PropertyMock()
        if endpoint == RunPipelineOperator.RUN_PIPELINE_PATH.format(pipeline_id):
            resp.content = '{{"entity_id": "{}"}}'.format(run_id)
        elif endpoint == RunPipelineOperator.GET_PIPELINE_RUN_PATH.format(run_id):
            resp.content = '{{"status": "{}"}}'.format(
                RunPipelineOperator.SUCCESS_STATUS[0]
            )
        elif endpoint == RunPipelineOperator.GET_ENVIRONMENT_PATH:
            resp.content = '[{{"public_id": "{}"}}]'.format(environment_id)
        else:
            pytest.fail("Called with bad args")
        return resp

    with patch.object(
        BedrockHook, "run", side_effect=bedrockhook_run_side_effect
    ) as mock_resp:
        op.execute(None)

    assert mock_resp.call_count >= 3

    # get_env_call, run_pipeline_call, check_status_call, *others = mock_resp.mock_calls
    get_env_call = mock_resp.mock_calls[0]
    run_pipeline_call = mock_resp.mock_calls[1]
    check_status_call = mock_resp.mock_calls[2]
    assert get_env_call[1][0] == RunPipelineOperator.GET_ENVIRONMENT_PATH
    assert run_pipeline_call[1][0] == RunPipelineOperator.RUN_PIPELINE_PATH.format(
        pipeline_id
    )
    assert check_status_call[1][0] == RunPipelineOperator.GET_PIPELINE_RUN_PATH.format(
        run_id
    )


def test_run_pipeline_waiting(airflow_connection):
    environment_id = "test-environment"
    pipeline_id = "some-pipeline-id"
    run_id = "some-run-id"

    dag = DAG("bedrock_dag", start_date=datetime.datetime.now())
    op = RunPipelineOperator(
        task_id="run_pipeline",
        dag=dag,
        conn_id=airflow_connection,
        pipeline_id=pipeline_id,
    )

    _outer = {"has_waited": False}

    def bedrockhook_run_side_effect(endpoint, *_args, **_kwargs):
        resp = PropertyMock()
        if endpoint == RunPipelineOperator.RUN_PIPELINE_PATH.format(pipeline_id):
            resp.content = '{{"entity_id": "{}"}}'.format(run_id)
        elif endpoint == RunPipelineOperator.GET_PIPELINE_RUN_PATH.format(run_id):
            if not _outer["has_waited"]:
                resp.content = '{{"status": "{}"}}'.format(
                    RunPipelineOperator.WAIT_STATUS[0]
                )
                _outer["has_waited"] = True
            else:
                resp.content = '{{"status": "{}"}}'.format(
                    RunPipelineOperator.SUCCESS_STATUS[0]
                )
        elif endpoint == RunPipelineOperator.GET_ENVIRONMENT_PATH:
            resp.content = '[{{"public_id": "{}"}}]'.format(environment_id)
        else:
            pytest.fail("Called with bad args")
        return resp

    with patch.object(
        BedrockHook, "run", side_effect=bedrockhook_run_side_effect
    ) as mock_resp, patch.object(time, "sleep"):
        op.execute(None)

    assert mock_resp.call_count >= 3

    # get_env_call, run_pipeline_call, check_status_call, *others = mock_resp.mock_calls
    get_env_call = mock_resp.mock_calls[0]
    run_pipeline_call = mock_resp.mock_calls[1]
    check_status_call = mock_resp.mock_calls[2]
    assert get_env_call[1][0] == RunPipelineOperator.GET_ENVIRONMENT_PATH
    assert run_pipeline_call[1][0] == RunPipelineOperator.RUN_PIPELINE_PATH.format(
        pipeline_id
    )
    assert check_status_call[1][0] == RunPipelineOperator.GET_PIPELINE_RUN_PATH.format(
        run_id
    )


def test_run_pipeline_failure(airflow_connection):
    environment_id = "test-environment"
    pipeline_id = "some-pipeline-id"
    run_id = "some-run-id"
    fail_status = "A huge failure"

    dag = DAG("bedrock_dag", start_date=datetime.datetime.now())
    op = RunPipelineOperator(
        task_id="run_pipeline",
        dag=dag,
        conn_id=airflow_connection,
        pipeline_id=pipeline_id,
    )

    def bedrockhook_run_side_effect(endpoint, *_args, **_kwargs):
        resp = PropertyMock()
        if endpoint == RunPipelineOperator.RUN_PIPELINE_PATH.format(pipeline_id):
            resp.content = '{{"entity_id": "{}"}}'.format(run_id)
        elif endpoint == RunPipelineOperator.GET_PIPELINE_RUN_PATH.format(run_id):
            resp.content = '{{"status": "{}"}}'.format(fail_status)
        elif endpoint == RunPipelineOperator.GET_ENVIRONMENT_PATH:
            resp.content = '[{{"public_id": "{}"}}]'.format(environment_id)
        elif endpoint == RunPipelineOperator.STOP_PIPELINE_RUN_PATH.format(run_id):
            resp.content = "OK"
        else:
            pytest.fail("Called with bad args: {}".format(endpoint))
        return resp

    with patch.object(
        BedrockHook, "run", side_effect=bedrockhook_run_side_effect
    ) as mock_resp, pytest.raises(Exception) as ex:
        op.execute(None)

    assert ex.value.args[0] == "Run status is {}".format(fail_status)
    assert mock_resp.call_count >= 4

    # get_env_call, run_pipeline_call, check_status_call, stop_run_call, *others = (
    #     mock_resp.mock_calls
    # )
    get_env_call = mock_resp.mock_calls[0]
    run_pipeline_call = mock_resp.mock_calls[1]
    check_status_call = mock_resp.mock_calls[2]
    stop_run_call = mock_resp.mock_calls[3]
    assert get_env_call[1][0] == RunPipelineOperator.GET_ENVIRONMENT_PATH
    assert run_pipeline_call[1][0] == RunPipelineOperator.RUN_PIPELINE_PATH.format(
        pipeline_id
    )
    assert check_status_call[1][0] == RunPipelineOperator.GET_PIPELINE_RUN_PATH.format(
        run_id
    )
    assert stop_run_call[1][0] == RunPipelineOperator.STOP_PIPELINE_RUN_PATH.format(
        run_id
    )
