"""
Kafka Connection Module
Author: Po-Chun, Lu
"""
import json
from collections import namedtuple

from loguru import logger
from confluent_kafka import Consumer, KafkaException, KafkaError

from config import CONFIG


def _error_cb(err):
    logger.error("Error: %s" % err)


def _decode_utf8(data):
    if data:
        return data.decode("utf-8")

    return None


class KafkaConsumer:
    """ Activate a Kafka consumer instance

    Args:
        config: arguments for Kafka consumer
    Attributes:
        topic_names (:obj:`list` of :obj:`str`): topics to subscribe e.g. ['command', 'get', 'insert']
        consumer (:obj:`instance`): a confluent_kafka Consumer instance

    """

    def __init__(self):
        config = CONFIG["consumer_kafka"]

        kafka_config = {
            "bootstrap.servers": config["kafka_ip"],
            "group.id": config["group_ip"],
            "auto.offset.reset": "earliest",
            "session.timeout.ms": config["session_timeout"],
            "error_cb": _error_cb,
        }

        self.consumer = Consumer(kafka_config)
        self.topic_names = config["topic_names"]

    def start(self):
        """start the kafka consumer service
        """
        self.consumer.subscribe(self.topic_names)
        logger.info(f"Monitor topics: {self.topic_names}")

    def _get_msgs_from_queue(self):
        records = self.consumer.consume(num_messages=500, timeout=1.0)
        return records

    @staticmethod
    def _get_info_gen_from_msgs(records):
        def get_info_from_msg(record):
            topic = record.topic()
            timestamp = record.timestamp()
            msg_key = _decode_utf8(record.key())
            msg_value = _decode_utf8(record.value())

            MsgInfo = namedtuple(
                "MsgInfo", ["topic", "msg_key", "msg_value", "timestamp"]
            )

            return MsgInfo(topic, msg_key, json.loads(msg_value), timestamp)

        for record in records:
            if record is None:
                continue
            if record.error():
                # pylint: disable=W0212
                # (protected-access)
                # Error or event
                if record.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                    logger.warning(
                        f"\n {record.topic()} {record.partition()} reached end at offset {record.offset()}"
                    )
                else:
                    # Error
                    raise KafkaException(record.error())
                # pylint: enable=W0212
            else:
                yield get_info_from_msg(record)

    def get_info_gen_from_queue(self):
        """ retrieve data from msg queue, ignore blank msg

        Return:
            generator object of msg data

        Examples:
            >>> print([record for record in consumer.get_validated_data()])
            [
                MsgInfo(topic='command', msg_key='key', msg_value='value', timestamp=(1, 1554436613182)),
                MsgInfo(topic='command', msg_key='key', msg_value='value', timestamp=(1, 1554436613182))
            ]
        """
        records = self._get_msgs_from_queue()
        validated_records = self._get_info_gen_from_msgs(records)
        return validated_records

    def close(self):
        """close the kafka consumer service
        """
        self.consumer.close()
