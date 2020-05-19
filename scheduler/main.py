"""

Author: Po-Chun, Lu
"""
from loguru import logger

from connector.msg_queue.kafka import KafkaConsumer
from operators.job_consumer.main import JobConsumer
from operators.job_monitor.main import JobMonitor


class MainProcess:
    """ Entry Process of Job scheduling
    """

    def __init__(self) -> None:

        # for getting msg
        self.consumer = KafkaConsumer()

        # for processing msg
        self.operator = JobConsumer(JobMonitor())

    def _handle_msgs(self) -> None:
        while True:
            msgs = self.consumer.get_info_gen_from_queue()
            for msg in msgs:

                logger.info(
                    f"Get MSG \n - Topic: {msg.topic}, \n - Key: {msg.msg_key}\n - Value: {msg.msg_value}"
                )

                self.operator.consume_msg(msg)

    def run(self) -> None:
        """ start msg queue consumer and consume msgs
        """
        try:
            self.consumer.start()
            self._handle_msgs()

        except KeyboardInterrupt:
            logger.warning("Aborted by user")
        finally:
            self.consumer.close()


def main():
    """ define main function for cython usage
    """
    logger.info("ReStart Scheduler Process")
    app = MainProcess()
    app.run()


if __name__ == "__main__":
    main()
