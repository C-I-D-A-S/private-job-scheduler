"""
Single Staging Queue Module
Author: Po-Chun, Lu
"""
from typing import List
import heapq

from operators.job_consumer.base_job import Job


class StagingList:
    """ Job Queue for buffering each level job request before assigning to worker
    """

    def __init__(self, level: int) -> None:
        """
        Arguments:
            level {int} -- importance of this list
        """
        self.level = level

        # for job storaging
        self.heap: List[Job] = []

    def insert(self, job: Job) -> None:
        """ insert the latest job into this heap
        """
        heapq.heappush(self.heap, job)

    def pop(self) -> Job:
        """ get the most urgent job for worker to operate
        """
        return heapq.heappop(self.heap)

    def renew_jobs_priority(self) -> None:
        """ recompute the job priority since the scheduling time would change
        """
        self.heap = list(map(lambda job: job.renew_priority(), self.heap))

    def sort(self) -> None:
        """ use heapsort for staging list sorting
        """
        heapq.heapify(self.heap)
