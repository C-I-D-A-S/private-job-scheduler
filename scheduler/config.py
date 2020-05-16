""" config for scheduler module
"""
import os
from typing import Tuple
from mypy_extensions import TypedDict

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

DATE_FORMAT = os.environ.get("DATE_FORMAT", "%Y-%m-%d %H:%M:%S")

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
        "env_weights": map(
            float, os.environ.get("SELECT_WEIGHT", "0.5,0.85").split(",")
        )
    },
}

JOB_SELECTION_CONFIG = {
    "JOB_SELECT_METHOD": os.environ.get("JOB_SELECT_METHOD", "basic_check_resource")
}

QUEUE_SCHEDULE_CONFIG = {"STAGE_QUEUE": os.environ.get("STAGE_QUEUE", "heap")}
