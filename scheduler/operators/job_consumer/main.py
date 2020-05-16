"""
Entry Module for handling coming jobs
Author: Po-Chun, Lu
"""
from typing import Tuple

from config import KAFKA_TOPIC_CONFIG, SCHEDULER_CONFIG
from operators.job_monitor.main import JobMonitor
from operators.job_consumer.resources.base_job import Job
from operators.job_consumer.resources import STAGING_LIST
from operators.job_consumer.plugins import QUEUE_SELECTOR, JOB_SELECTOR


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
        # setup job cpu & mem usage based on system status
        job.resources = self.job_monitor.get_single_job_resources(job)

        if not job.resources:
            return

        job_level = self._extract_job_level(job)
        if SCHEDULER_CONFIG["IS_RENEW_BEFORE_INSERT"]:
            self.stage_lists[job_level].renew_jobs_priority()

        self.stage_lists[job_level].insert(job)

    def _pick_next_job(self) -> Job:
        """ When spark executor is free, it would pick a job which is the top priority of computing

        Returns:
            Job -- [The next job that would be assign to airflow and spark]
        """
        system_resources = self.job_monitor.fetch_current_system_resources_from_api()
        next_queue = QUEUE_SELECTOR.select_queue(self.stage_lists)

        return JOB_SELECTOR.select_job(next_queue.tolist(), system_resources)

    def _send_job_to_airflow(self):
        # TODO: implement send to airflow
        next_job = self._pick_next_job()
        return next_job

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
        elif msg.topic == KAFKA_TOPIC_CONFIG["TOPIC_NEW_JOB_NOTIFY"]:
            self._send_job_to_airflow()

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
