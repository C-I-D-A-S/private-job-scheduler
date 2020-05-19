"""
Module for monitor system valid resources of spark and assign resources for jobs
Author: Po-Chun, Lu
"""
from typing import Dict, Optional

from loguru import logger

from operators.job_consumer.resources.base_job import Job


class JobMonitor:
    """ monitor system resources and allocate job resources
    """

    def __init__(self):
        self.jobs_resources = self._fetch_job_resources_from_api()

    @staticmethod
    def _fetch_job_resources_from_api() -> Dict[str, Dict]:
        """ Get Job related resource requirements

        Returns:
            Dict[str, Dict] -- cpu cores and memory usage(G) and mapping computing time of all jobs
                               e.g. {"demand_forecasting": {"cpu": 1, "mem": 2, "computing_time": 3}}
        """
        # TODO: get min resources from db
        return {
            "demand_forecasting_1hr": {
                "executors": 2,
                "cpu": 1,
                "mem": 1,
                "computing_time": 5,
            }
        }

    def get_single_job_resources(self, job: Job) -> Optional[Dict[str, int]]:
        """[summary]

        Arguments:
            job {Job} -- [The next job that would be assign to airflow and spark]

        Returns:
            Dict[str, int] -- cpu cores and memory usage(G) of this job
                              e.g. {"cpu": 1, "mem": 2, "computing_time": 3}
        """
        try:
            job_resource = self.jobs_resources[job.job_type]
            return job_resource

        except KeyError as err:
            logger.error(f"Job Resources not exist: {err}")
            return None

    @staticmethod
    def fetch_current_system_resources_from_api() -> Dict[str, Dict]:
        """Get Spark System Valid Resources

        Returns:
            Dict[str, Dict] -- e.g. {
                "total": {"cpu": 3, "mem": 6},
                "max": {"cpu": 8, "mem": 16},
                "0": {"cpu": 8, "mem": 16},
                "1": {"cpu": 7, "mem": 16},
                ...
            }
        """

        # TODO: get system resources from spark
        return {
            "max": {"cpu": 8, "mem": 16},
            "0": {"cpu": 8, "mem": 16},
            "1": {"cpu": 7, "mem": 12},
        }
