""" config for scheduler module
"""
import os

CONFIG = {
    "consumer_kafka": {
        "kafka_ip": os.environ.get("KAFKA_IP", "localhost:9092"),
        "group_ip": os.environ.get("GROUP_ID", "qol"),
        "session_timeout": 6000,
        "topic_names": [os.environ.get("TOPIC_NAME_NOTIFICATION", "notification")],
    }
}


DATE_FORMAT = os.environ.get("DATE_FORMAT", "%Y-%m-%d %H:%M:%S")

SCHEDULER_CONFIG = {
    # TOTAL_LEVEL: 3 -> there would be 3 staging queue
    "TOTAL_LEVEL": os.environ.get("TOTAL_LEVEL", 3),
    "LEVEL_LIMIT": map(int, os.environ.get("LEVEL_LIMIT", "600,1200").split(",")),
    # Queue config
    "IS_RENEW_BEFORE_INSERT": bool(os.environ.get("IS_RENEW_BEFORE_INSERT", 1)),
    "JOB_SORT_KEY": os.environ.get("JOB_SORT_KEY", "schedule_time"),
}

QUEUE_SELECTION_CONFIG = {
    "QUEUE_SELECT_METHOD": os.environ.get(
        "QUEUE_SELECT_METHOD", "env_weight_random_select"
    ),
    "env_weight_random_select": {
        "env_weights": map(
            float, os.environ.get("SELECT_WEIGHT", "0.5,0.85").split(",")
        ),
    },
}
