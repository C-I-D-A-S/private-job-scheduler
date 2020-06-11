""" config for scheduler module
"""
import sys
import os
from typing import Tuple

from loguru import logger
from dotenv import load_dotenv
from mypy_extensions import TypedDict


logger.remove()


def formatter(record):
    """ log text formatter
        10: Debug, 20: INFO, 25: SUCCESS, 30: WARNING, 40: ERROR, 50: CRITICAL
    """
    time_str = "<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</> | "
    formate_map = {
        10: "<blue><b>{level: <8}</b></> | - <blue><b>{message}</b></>\n",
        20: "<b>{level: <8}</b> | - <b>{message}</b>\n",
        25: "<green><b>{level: <8}</b></> | - <green><b>{message}</b></>\n",
        30: "<yellow><b>{level: <8}</b></> | - <yellow><b>{message}</b></>\n",
        40: "<red><b>{level: <8}</b></> | - <red><b>{message}</b></>\n",
        50: "<bg red><w><b>{level: <8}</b></></> | - <bg red><w><b>{message}</b></></>\n",
    }

    return time_str + formate_map[record["level"].no]


logger.add(sys.stderr, format=formatter)


load_dotenv()

SYSTEM_CONFIG = {
    "SYSTEM_CPU": int(os.environ.get("SYSTEM_CPU", 1)),
    "SYSTEM_MEM": int(os.environ.get("SYSTEM_MEM", 1)),
}

KAFKA_TOPIC_CONFIG = {
    "TOPIC_NEW_JOB_NOTIFY": os.environ.get("TOPIC_NEW_JOB_NOTIFY", "new_job"),
    "TOPIC_JOB_COMPLETE_NOTIFY": os.environ.get("JOB_COMPLETE_NOTIFY", "job_finish"),
}

CONFIG = {
    "consumer_kafka": {
        "kafka_ip": os.environ.get("KAFKA_IP", "localhost:9092"),
        "group_ip": os.environ.get("GROUP_ID", "qol"),
        "session_timeout": 6000,
        "topic_names": [
            KAFKA_TOPIC_CONFIG["TOPIC_NEW_JOB_NOTIFY"],
            KAFKA_TOPIC_CONFIG["TOPIC_JOB_COMPLETE_NOTIFY"],
        ],
    }
}

AIRFLOW_CONFIG = {
    "URL": os.environ.get(
        "AIRFLOW_URL",
        "http://localhost:8080/api/experimental/dags/basic_test_job/dag_runs",
    )
}

JOB_TRIGGER_CONFIG = {
    "URL": os.environ.get("JOB_TRIGGER_URL", "http://localhost:5000/trigger/spark"),
    "METHOD": os.environ.get("JOB_TRIGGER_METHOD", "api"),
}

DATE_FORMAT = os.environ.get("DATE_FORMAT", "%Y-%m-%dT%H:%M:%S")

TYPE_SCHEDULER_CONFIG = TypedDict(
    "TYPE_SCHEDULER_CONFIG",
    {
        "TOTAL_LEVEL": int,
        "LEVEL_LIMIT": Tuple[int, ...],
        "IS_RENEW_BEFORE_INSERT": bool,
        "IS_REALLOCATE": bool,
        "JOB_SORT_KEY": str,
    },
    total=False,
)

SCHEDULER_CONFIG: TYPE_SCHEDULER_CONFIG = {
    # TOTAL_LEVEL: 3 -> there would be 3 staging queue
    "TOTAL_LEVEL": int(os.environ.get("TOTAL_LEVEL", 3)),
    "LEVEL_LIMIT": tuple(
        map(int, os.environ.get("LEVEL_LIMIT", "600,1200").split(","))
    ),
    # Queue config
    "IS_RENEW_BEFORE_INSERT": bool(os.environ.get("IS_RENEW_BEFORE_INSERT", 1)),
    "IS_REALLOCATE": bool(os.environ.get("IS_REALLOCATE", 1)),
    "JOB_SORT_KEY": os.environ.get("JOB_SORT_KEY", "schedule_time"),
}

QUEUE_SELECTION_CONFIG = {
    "QUEUE_SELECT_METHOD": os.environ.get(
        "QUEUE_SELECT_METHOD", "env_weight_random_select"
    ),
    "env_weight_random_select": {
        "env_weights": list(
            map(float, os.environ.get("SELECT_WEIGHT", "10,7,3").split(","))
        )
    },
    "env_zip_select": {
        "env_orders": list(
            map(float, os.environ.get("SELECT_ORDER", "3,2,1").split(","))
        )
    },
}

JOB_SELECTION_CONFIG = {
    "JOB_SELECT_METHOD": os.environ.get("JOB_SELECT_METHOD", "basic_check_resource")
}

QUEUE_SCHEDULE_CONFIG = {"STAGE_QUEUE": os.environ.get("STAGE_QUEUE", "heap")}

EXP_ID = (
    f"{os.environ.get('EXP_ID', '0.0.0')}"
    + f"_c{SYSTEM_CONFIG['SYSTEM_CPU']}_m{SYSTEM_CONFIG['SYSTEM_MEM']}"
    + f"_queueSelect-{QUEUE_SELECTION_CONFIG['QUEUE_SELECT_METHOD']}"
    + f"_queue-{QUEUE_SCHEDULE_CONFIG['STAGE_QUEUE']}"
)

logger.info(EXP_ID)


def get_exp_config():
    """ for exp analysis table """
    return {
        "method": {
            "SCHEDULER_CONFIG": SCHEDULER_CONFIG,
            "QUEUE_SELECT_METHOD": QUEUE_SELECTION_CONFIG["QUEUE_SELECT_METHOD"],
            "QUEUE_SCHEDULE_CONFIG": QUEUE_SCHEDULE_CONFIG,
            "JOB_SELECTION_CONFIG": JOB_SELECTION_CONFIG,
        },
        "exp_id": EXP_ID,
    }
