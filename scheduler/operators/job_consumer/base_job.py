"""
Single Job Module
Author: Po-Chun, Lu
"""
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
        self.job_config = {
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
        self.schedule_time = (
            self.job_config["deadline"] - self.job_config["request_time"]
        ).seconds
        self.sort_key = getattr(self, sort_key)

    def __lt__(self, other) -> None:
        """ For sorting usage
        """
        return self.sort_key < other.sort_key

    def __str__(self):
        return ",".join((self.job_id, self.job_type, str(self.sort_key)))

    def renew_priority(self) -> object:
        """ When a new job coming, we need to recompute the scheduling time before insert the new job into staging list
        """
        self.schedule_time = (self.job_config["deadline"] - datetime.now()).seconds
        return self
