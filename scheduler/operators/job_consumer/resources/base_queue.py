"""
Single Staging Queue Module
Author: Po-Chun, Lu
"""
import abc
from typing import List, Deque
from collections import deque
import heapq
import bisect

from config import QUEUE_SCHEDULE_CONFIG
from operators.job_consumer.resources.base_job import Job


class BaseStagingList:
    """ Job Queue for buffering each level job request before assigning to worker
        Basic List Version
    """

    def __init__(self, level):
        """
        Arguments:
            level {int} -- importance of this list
        """
        self.level = level

        # for job storaging
        self.job_list: List[Job] = []

    @abc.abstractmethod
    def insert(self, job: Job) -> None:
        """ insert new job to job queue """
        return NotImplemented

    @abc.abstractmethod
    def pop(self) -> Job:
        """ pop job from job queue """
        return NotImplemented

    def renew_jobs_priority(self) -> None:
        """ recompute the job priority since the scheduling time would change
        """
        self.job_list = list(map(lambda job: job.renew_priority(), self.job_list))

    def tolist(self) -> List[Job]:
        """ return a priority sorted list for job selector iterating and pick a valid job
        """
        return self.job_list


class DequeStagingList:
    """ Job Queue for buffering each level job request before assigning to worker
        Deque Version
    """

    def __init__(self, level: int) -> None:
        """
        Arguments:
            level {int} -- importance of this list
        """
        self.level = level

        # for job storaging
        self.job_list: Deque[Job] = deque([])

    def insert(self, job: Job) -> None:
        """ insert the latest job into this list
        """
        self.job_list.append(job)

    def pop(self) -> Job:
        """ get the most urgent job for worker to operate
        """
        return self.job_list.popleft()

    def renew_jobs_priority(self) -> None:
        """ recompute the job priority since the scheduling time would change
        """
        self.job_list = deque(map(lambda job: job.renew_priority(), self.job_list))

    def tolist(self) -> Deque[Job]:
        """ return deque for job selector iterating and pick a valid job
        """
        return self.job_list


class HeapStagingList:
    """ Staging List Based on Heap
    """

    def __init__(self, level: int) -> None:
        """
        Arguments:
            level {int} -- importance of this list
        """
        self.level = level

        # for job storaging
        self.job_list: List[Job] = []

    def insert(self, job: Job) -> None:
        """ insert the latest job into this heap
        """
        heapq.heappush(self.job_list, job)

    def pop(self) -> Job:
        """ get the most urgent job for worker to operate
        """
        return heapq.heappop(self.job_list)

    def sort(self) -> None:
        """ use heapsort for staging list sorting
        """
        heapq.heapify(self.job_list)

    def renew_jobs_priority(self) -> None:
        """ recompute the job priority since the scheduling time would change
        """
        self.job_list = list(map(lambda job: job.renew_priority(), self.job_list))

    def tolist(self) -> List[Job]:
        """ return sorted list type for job selector iterating and pick a valid job
        """
        return sorted(self.job_list)


class BisectStagingList(BaseStagingList):
    """ Staging list based on bisect insort
    """

    def insert(self, job: Job) -> None:
        """ insert the latest job into this heap
        """
        bisect.insort_right(self.job_list, job)

    def pop(self) -> Job:
        """ get the most urgent job for worker to operate
        """
        return self.job_list.pop()

    def remove(self, job: Job):
        """ remove specific job from list
        """
        self.job_list.remove(job)


def get_staging_list():
    """ Choose Type of staging list based on .env
        Each staging list get diff sort method or data structure
    """
    queue_map = {
        "deque": DequeStagingList,
        "heap": HeapStagingList,
        "bisect": BisectStagingList,
    }

    return queue_map[QUEUE_SCHEDULE_CONFIG["STAGE_QUEUE"]]
