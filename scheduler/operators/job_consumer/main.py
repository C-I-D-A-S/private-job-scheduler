"""
Entry Module for handling coming jobs
Author: Po-Chun, Lu
"""

from config import SCHEDULER_CONFIG

from operators.job_consumer.resources.base_queue import StagingList
from operators.job_consumer.resources.base_job import Job


class JobConsumer:
    """ Operator for consuming job object and send job object to its staging list
    """

    def __init__(self):

        self.total_level = SCHEDULER_CONFIG["TOTAL_LEVEL"]
        self.level_limit = SCHEDULER_CONFIG["LEVEL_LIMIT"]

        # init all staging queue
        self.stage_list = [StagingList(level) for level in range(self.total_level)]

    def _extract_job_level(self, job: Job) -> int:
        """ Check the importance level (priority) of this job
            e.g. level_limit = (600,1200) and job_sort_key = 100, then job_level is 0
        Arguments:
            job {Job} -- Data Analysis Job

        Returns:
            int -- importance level of this job (0,1,2,3....)
        """
        for job_level, limit in enumerate(self.level_limit):
            if job.sort_key < limit:
                return job_level

        # since job_level start from 0, total level 3 -> the biggest level is 2
        return self.total_level - 1

    def _consume_job(self, job: Job) -> None:
        job_level = self._extract_job_level(job)

        if SCHEDULER_CONFIG["IS_RENEW_BEFORE_INSERT"]:
            self.stage_list[job_level].renew_jobs_priority()

        self.stage_list[job_level].insert(job)

    def process_msg(self, msg) -> None:
        """ A common method for handling msg, used for Polymorphism

        Arguments:
            msg {namedtuple} -- msg retrieve from kafka consumer
        """
        self._consume_job(Job(job_msg=msg, sort_key=SCHEDULER_CONFIG["JOB_SORT_KEY"]))
