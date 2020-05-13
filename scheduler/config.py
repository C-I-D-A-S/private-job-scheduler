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
    "RENEW_BEFORE_INSERT": bool(os.environ.get("RENEW_BEFORE_INSERT", 1)),
}
