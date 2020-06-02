"""
Collection of job selectors
When spark executor is free, job selector would pick a job from a queue based on specific query method
Author: Po-Chun, Lu
"""
import abc
from typing import Dict, List, Optional

from loguru import logger

from config import JOB_SELECTION_CONFIG
from operators.job_consumer.resources.base_job import Job


class BaseJobSelector:
    """ For Job Selector Polymorphism
    """

    # pylint: disable=W0613
    @classmethod
    @abc.abstractclassmethod
    def select_job(cls, stage_list, system_resources) -> Job:
        """ common method of job selector
        """
        return NotImplemented

    # pylint: enable=W0613


class BasicJobSelector(BaseJobSelector):
    """ Just pick the first system resources avaliable job from the queue
    """

    @classmethod
    def select_job(cls, stage_list: List[Job], system_resources: Dict) -> Optional[Job]:
        """ pick the next system avaliable job

        Arguments:
            stage_list {Job} -- job queue
            system_resources {Dict} -- e.g. {
                "max": {"cpu": 8, "mem": 16},
                "0": {"cpu": 8, "mem": 16},
                "1": {"cpu": 7, "mem": 12},
                ...
            }

        Returns:
            Job -- The next job that would be execute
        """
        # pylint: disable=C0330
        if len(stage_list) == 0:
            return None

        for job in stage_list:
            if (
                job.job_resources["cpu"] <= system_resources["total"]["cpu"]
                and job.job_resources["mem"] <= system_resources["total"]["mem"]
            ):
                next_job = job
                break
        else:
            logger.warning(f"NO VALID JOB - SYSTEM: {system_resources}")
            next_job = None
        # pylint: enable=C0330

        return next_job


def get_job_selector():
    """ Organize the selectors
        select a queue selector based on .env
    """
    selector_map = {
        "basic_pick_first": BaseJobSelector,
        "basic_check_resource": BasicJobSelector,
    }

    return selector_map[JOB_SELECTION_CONFIG["JOB_SELECT_METHOD"]]
