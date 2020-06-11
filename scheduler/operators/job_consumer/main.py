"""
Entry Module for handling coming jobs
Author: Po-Chun, Lu
"""
from typing import Tuple, List, Dict

from loguru import logger

from config import (
    KAFKA_TOPIC_CONFIG,
    SCHEDULER_CONFIG,
)
from operators.job_monitor.main import JobMonitor
from operators.job_consumer.resources.base_job import Job
from operators.job_consumer.resources import STAGING_LIST
from operators.job_consumer.plugins import QUEUE_SELECTOR, JOB_SELECTOR, SEND_JOB
from operators.job_consumer.plugins.job_selector.exceptions import (
    EmptyListException,
    NoValidJobInListException,
    NoValidJobInAllListException,
)


class JobConsumer:
    """ Operator for consuming job object and send job object to its staging list
    """

    def __init__(self, job_monitor: JobMonitor):
        # for monitor system resources
        self.job_monitor = job_monitor

        self.total_level: int = SCHEDULER_CONFIG["TOTAL_LEVEL"]
        self.level_limit: Tuple[int, ...] = SCHEDULER_CONFIG["LEVEL_LIMIT"]

        # init all staging queue
        self.stage_lists = [STAGING_LIST(level) for level in range(self.total_level)]

    def _extract_job_level(self, job: Job) -> int:
        """ Check the importance level (priority) of this job
            e.g. level_limit = (600,1200) and job_sort_key = 100, then job_level is 0
        Arguments:
            job {Job} -- Data Analysis Job

        Returns:
            int -- importance level of this job (0,1,2,3....)
        """
        limit: int
        job_level: int
        for job_level, limit in enumerate(self.level_limit):
            if job.sort_key < limit:
                return job_level

        # since job_level start from 0, total level 3 -> the biggest level is 2
        return self.total_level - 1

    def _consume_job(self, job: Job) -> None:
        try:
            # setup job cpu & mem usage based on system status
            job.job_resources = self.job_monitor.get_single_job_resources(job)
        except ValueError:
            return

        # TODO: Remove for Prod
        if job.job_params["resources"]:
            # get resource from user
            job.job_resources = job.job_params["resources"]
        else:
            # update scheduler time by house num
            num = job.job_params["num"]
            job.job_resources["computing_time"] = int(((num - 50) / 50) * 15 + 30)

        # ori: deadline - request_time, new: deadline - request_time - computing_time
        job.job_times["schedule_time"] -= job.job_resources["computing_time"]
        logger.debug(f'schedule_time: {job.job_times["schedule_time"]}')

        job_level = self._extract_job_level(job)
        if SCHEDULER_CONFIG["IS_RENEW_BEFORE_INSERT"]:
            self.stage_lists[job_level].renew_jobs_priority()

        self.stage_lists[job_level].insert(job)

    def reallocate(self) -> None:
        """ move job from low level stage queue to high level stage queue
        """
        for level, stage_list in enumerate(self.stage_lists):
            self.stage_lists[level].renew_jobs_priority()

            if level != 0:
                # Check whether the real level of a job is changed
                while (level) > self._extract_job_level(stage_list[0]):
                    job = stage_list.pop()
                    self.stage_lists[level].insert(job)

    def _re_pick_next_valid_job(
        self, valid_queues: List[int], system_resources: Dict
    ) -> Job:
        candidate_queues = valid_queues.copy()
        for queue_level in valid_queues:
            if len(self.stage_lists[queue_level].job_list) == 0:
                candidate_queues.remove(queue_level)

        logger.warning(f"Other Non Empty Queues: {candidate_queues}")
        for queue_level in candidate_queues:
            try:
                next_queue = self.stage_lists[queue_level]
                next_job = JOB_SELECTOR.select_job(
                    next_queue.tolist(), system_resources
                )
                next_queue.remove(next_job)
                logger.warning(f"Final Pick Level {next_queue.level}")
                break
            except NoValidJobInListException as error:
                logger.warning(f"Level {queue_level} {error}")
                continue
        else:
            raise NoValidJobInAllListException

        return next_job

    def _handle_no_valid_job_in_current_list(
        self, invalid_queue_level, system_resources
    ):
        try:
            valid_queues = list(range(self.total_level))
            valid_queues.pop(invalid_queue_level)

            next_job = self._re_pick_next_valid_job(valid_queues, system_resources)
            return next_job

        except NoValidJobInAllListException:
            raise EmptyListException

    def _pick_next_job(self) -> Job:
        """ When spark executor is free, it would pick a job which is the top priority of computing

        Returns:
            Job -- [The next job that would be assign to airflow and spark]
        """
        system_resources = self.job_monitor.fetch_current_system_resources_from_api()
        next_queue = QUEUE_SELECTOR.select_queue(self.stage_lists)
        logger.info(
            f"Current Queue - Level: {next_queue.level}, Length: {len(next_queue.job_list)}"
        )

        try:
            next_job = JOB_SELECTOR.select_job(next_queue.tolist(), system_resources)
            next_queue.remove(next_job)

        except EmptyListException:
            raise

        except NoValidJobInListException as error:
            logger.warning(f"Level {next_queue.level} {error}")
            # next_job pop out inside
            next_job = self._handle_no_valid_job_in_current_list(
                next_queue.level, system_resources
            )

        return next_job

    def _send_job_to_trigger(self):
        try:
            next_job = self._pick_next_job()

        except EmptyListException:
            logger.warning("No staging or valid job in all queues")
            for queue in self.stage_lists:
                logger.debug(
                    f"- Queue Level: {queue.level}, Job Num: {len(queue.job_list)}"
                )

            return "empty"

        logger.info(
            f"Pick Job:\n Resources: \n{next_job.job_resources}, \n Time: \n{next_job.job_times}"
        )
        SEND_JOB(next_job)

        self.job_monitor.update_current_system_resources(
            -next_job.job_resources["cpu"], -next_job.job_resources["mem"]
        )

        return ""

    def consume_msg(self, msg) -> None:
        """ A common method for handling msg, used for Polymorphism

        Arguments:
            msg {namedtuple} -- msg retrieve from kafka consumer
                                include ["topic", "msg_key", "msg_value", "timestamp"]
        """
        if msg.topic == KAFKA_TOPIC_CONFIG["TOPIC_NEW_JOB_NOTIFY"]:
            self._consume_job(
                Job(job_msg=msg, sort_key=SCHEDULER_CONFIG["JOB_SORT_KEY"])
            )

            session = ""
            while (
                self.job_monitor.system_resources["total"]["cpu"] >= 1
                and session != "empty"
            ):
                session = self._send_job_to_trigger()

            if self.job_monitor.system_resources["total"]["cpu"] < 1:
                logger.warning(
                    f"No more resources : {self.job_monitor.system_resources}"
                )

        elif msg.topic == KAFKA_TOPIC_CONFIG["TOPIC_JOB_COMPLETE_NOTIFY"]:
            self.job_monitor.update_current_system_resources(
                msg.msg_value["cpu"], msg.msg_value["mem"]
            )
            self._send_job_to_trigger()
