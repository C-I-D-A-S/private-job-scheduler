import json
from datetime import datetime

from loguru import logger

from config import (
    AIRFLOW_CONFIG,
    JOB_TRIGGER_CONFIG,
    DATE_FORMAT,
    get_exp_config,
)
from utils.common import send_post_request


def send_job_to_none(next_job) -> None:
    """ For Local Testing
    """
    logger.success(f'\n{"-"*20}\nFake send success \n{"-"*20}')


def send_job_to_airflow(next_job) -> None:
    send_post_request(
        url=f'{AIRFLOW_CONFIG["URL"]}',
        headers={"Cache-Control": "no-cache", "Content-Type": "application/json"},
        data=json.dumps(
            {
                "conf": {
                    "job_id": next_job.job_id,
                    "job_type": next_job.job_type,
                    "job_params": json.dumps(next_job.job_params),
                    "job_times": json.dumps(next_job.job_times),
                    "resources": json.dumps(next_job.job_resources),
                    "num": next_job.job_params["num"],
                    "request_time": datetime.strftime(
                        next_job.job_times["request_time"], DATE_FORMAT
                    ),
                    "deadline": datetime.strftime(
                        next_job.job_times["deadline"], DATE_FORMAT
                    ),
                    "executors": 1,
                    "cpu": 1,
                    "mem": 1,
                    "computing_time": next_job.job_resources["computing_time"],
                }
            }
        ),
    )


def send_job_to_job_trigger(next_job) -> None:
    job_times = {
        key: datetime.strftime(next_job.job_times[key], DATE_FORMAT)
        if key != "schedule_time"
        else next_job.job_times[key]
        for key in next_job.job_times
    }
    send_post_request(
        url=f'{JOB_TRIGGER_CONFIG["URL"]}',
        headers={"Cache-Control": "no-cache", "Content-Type": "application/json"},
        data=json.dumps(
            {
                "job_id": next_job.job_id,
                "job_type": next_job.job_type,
                "job_params": {**next_job.job_params, **get_exp_config()},
                "job_times": job_times,
                "job_resources": next_job.job_resources,
            }
        ),
    )


def get_job_trigger():
    """ Organize the triggers
        select a queue selector based on .env
    """
    selector_map = {
        "test": send_job_to_none,
        "api": send_job_to_job_trigger,
        "airflow": send_job_to_airflow,
    }

    return selector_map[JOB_TRIGGER_CONFIG["METHOD"]]
