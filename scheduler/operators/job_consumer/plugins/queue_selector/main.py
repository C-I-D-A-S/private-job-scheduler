"""
Collection of queue selectors
When spark executor is free, queue selector would pick a queue which is the top priority
Author: Po-Chun, Lu
"""
import abc
import random

from config import QUEUE_SELECTION_CONFIG
from operators.job_consumer.resources import STAGING_LIST


class BaseQueueSelector:
    """ For Queue Selector Polymorphism
    """

    # pylint: disable=W0613
    @classmethod
    @abc.abstractclassmethod
    def select_queue(cls, stage_lists) -> STAGING_LIST:
        """ common method of queue selector
        """
        return NotImplemented

    # pylint: enable=W0613


class EnvWeightRandomSelect(BaseQueueSelector):
    """ set level ranges and pick a random number to choose queue
        e.g.level weight = L1: 0~0.5, L2: 0.5~0.85, L3: 0.85 ~ 1,
            random number = 0.4
            then pick L1 since 0.4 in 0~0.5

        level weight is set in .env directly
    """

    @staticmethod
    def _get_level_weight():
        # get weights from .env directly
        return QUEUE_SELECTION_CONFIG["env_weight_random_select"]["env_weights"]

    @classmethod
    def _get_queue_level(cls) -> int:
        random_weights = cls._get_level_weight()
        return random.choices(range(len(random_weights)), weights=random_weights)[0]

    @staticmethod
    def _get_level_weight_with_length(stage_lists):
        # prevent get a list with no job
        env_weights = QUEUE_SELECTION_CONFIG["env_weight_random_select"]["env_weights"]

        level_weights = [
            (1 if len(stage_list.job_list) > 0 else 0) * env_weights[i]
            for i, stage_list in enumerate(stage_lists)
        ]
        return level_weights

    @classmethod
    def _get_queue_level_with_length(cls, stage_lists) -> int:
        random_weights = cls._get_level_weight_with_length(stage_lists)
        return random.choices(range(len(random_weights)), weights=random_weights)[0]

    @classmethod
    def select_queue(cls, stage_lists) -> STAGING_LIST:

        queue_level = cls._get_queue_level()

        if len(stage_lists[queue_level].job_list) > 0:
            return stage_lists[queue_level]

        new_queue_level = cls._get_queue_level_with_length(stage_lists)
        return stage_lists[new_queue_level]


def get_queue_selector():
    """ Organize the selectors
        select a queue selector based on .env
    """
    selector_map = {"env_weight_random_select": EnvWeightRandomSelect}

    return selector_map[QUEUE_SELECTION_CONFIG["QUEUE_SELECT_METHOD"]]
