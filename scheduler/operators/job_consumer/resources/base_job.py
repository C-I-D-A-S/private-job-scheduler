"""
Single Job Module
Author: Po-Chun, Lu
"""
from typing import Dict, Any
from datetime import datetime

from config import DATE_FORMAT


class Job:
    """ class for storaging job related parameters
    """

    def __init__(self, job_msg, sort_key: str = "schedule_time") -> None:

        self.job_id = job_msg.msg_key
        self.job_type = job_msg.msg_value["job_type"]

        self.job_params = job_msg.msg_value["job_parameters"]

        job_config = job_msg.msg_value["job_config"]
        self.time_attr: Dict[str, Any] = {
            "deadline": datetime.strptime(job_config["deadline"], DATE_FORMAT),
            "request_time": datetime.strptime(job_config["request_time"], DATE_FORMAT),
        }

        # job resource requirement for executor
        self.requirements = {
            "require_cpu": None,
            "require_mem": None,
            "computing_time": None,
        }

        # for inner scheduling sorting
        self.time_attr["schedule_time"] = (
            self.time_attr["deadline"] - self.time_attr["request_time"]
        ).seconds
        self.sort_key = self.time_attr[sort_key]

    def __lt__(self, other) -> None:
        """ For sorting usage
        """
        return self.sort_key < other.sort_key

    def __str__(self):
        return ",".join((self.job_id, self.job_type, str(self.sort_key)))

    def _renew_schedule_time(self) -> None:
        self.time_attr["schedule_time"] = (
            self.time_attr["deadline"] - datetime.now()
        ).seconds

    def renew_priority(self) -> object:
        """ When a new job coming, we need to recompute the scheduling time before insert the new job into staging list
        """
        self._renew_schedule_time()
        return self

    def get_job_compute_requirement(self) -> None:
        pass
