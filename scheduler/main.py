"""

Author: Po-Chun, Lu
"""
from loguru import logger


from connector.msg_queue.kafka import KafkaConsumer
from operators.job_consumer.main import JobConsumer


class MainProcess:
    """ Entry Process of Job scheduling
    """

    def __init__(self) -> None:

        # for getting msg
        self.consumer = KafkaConsumer()
        # for processing msg
        self.operator = JobConsumer()

    def _handle_msgs(self) -> None:
        while True:
            msgs = self.consumer.get_info_gen_from_queue()
            for msg in msgs:

                logger.info(
                    f"Get MSG \n - Topic: {msg.topic}, Key: {msg.msg_key}, Value: {msg.msg_value}"
                )

                self.operator.process_msg(msg)

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
    logger.info(f"ReStart Scheduler Process")
    app = MainProcess()
    app.run()


if __name__ == "__main__":
    main()
