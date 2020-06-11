"""
Collection of queue selectors
When spark executor is free, queue selector would pick a queue which is the top priority
Author: Po-Chun, Lu
"""
from typing import List
import abc
import random

from loguru import logger

from config import QUEUE_SELECTION_CONFIG
from operators.job_consumer.resources import STAGING_LIST


class BaseQueueSelector:
    """ For Queue Selector Polymorphism
    """

    # pylint: disable=W0613
    @abc.abstractmethod
    def select_queue(self, stage_lists) -> STAGING_LIST:
        """ common method of queue selector
        """
        return NotImplemented

    # pylint: enable=W0613


class TopLevelQueueSelector(BaseQueueSelector):
    """ always choose L0, then L1, L2...
    """

    @classmethod
    def select_queue(cls, stage_lists) -> STAGING_LIST:

        for stage_list in stage_lists:
            if len(stage_list.job_list) > 0:
                return stage_list

        return stage_lists[0]


class EnvWeightRandomSelect(BaseQueueSelector):
    """ set level ranges and pick a random number to choose queue
        e.g.level weight = L1: 0~0.5, L2: 0.5~0.85, L3: 0.85 ~ 1,
            random number = 0.4
            then pick L1 since 0.4 in 0~0.5

        level weight is set in .env directly
    """

    @staticmethod
    def _pick_item_with_weights(random_weights):
        return random.choices(range(len(random_weights)), weights=random_weights)[0]

    @staticmethod
    def _get_level_weight() -> List[int]:
        # get weights from .env directly
        return QUEUE_SELECTION_CONFIG["env_weight_random_select"]["env_weights"]

    @classmethod
    def _get_queue_level(cls) -> int:
        random_weights = cls._get_level_weight()
        return cls._pick_item_with_weights(random_weights)

    @classmethod
    def _get_level_weight_with_length(cls, stage_lists) -> List[int]:
        """ get level weights with consideration of queue length
            e.g.    len of each queue: 3, 0, 2
                    weight of each queue: 0.5, 0.35, 0.15
                    final weights: 1*0.5, 0*0.35, 1*0.15
        """
        # prevent get a list with no job
        env_weights = cls._get_level_weight()

        level_weights = [
            (1 if len(stage_list.job_list) > 0 else 0) * env_weights[i]
            for i, stage_list in enumerate(stage_lists)
        ]
        return level_weights

    @classmethod
    def _get_queue_level_with_length(cls, stage_lists) -> int:
        random_weights = cls._get_level_weight_with_length(stage_lists)
        return cls._pick_item_with_weights(random_weights)

    @classmethod
    def select_queue(cls, stage_lists) -> STAGING_LIST:

        queue_level = cls._get_queue_level()

        if len(stage_lists[queue_level].job_list) > 0:
            return stage_lists[queue_level]

        new_queue_level = cls._get_queue_level_with_length(stage_lists)
        return stage_lists[new_queue_level]


class WeightRandomSelect(EnvWeightRandomSelect):
    """ set level ranges and pick a random number to choose queue
        level range based on queue states
    """

    @staticmethod
    def _get_level_weight():
        # get weights from calculations
        pass


class EnvZipSelect(BaseQueueSelector):
    """ select next queue based on specific order, the order is set in .env
        e.g. 3,2,1 -> 0,0,0,1,1,2
    """

    def __init__(self) -> None:
        # e.g. 3,2,1
        self.queue_order = QUEUE_SELECTION_CONFIG["env_zip_select"]["env_orders"]
        # which queue level
        self.cross_queue_cursor = 0
        # how much times
        self.curr_queue_cursor = 1

    def _update_queue_cursor_level(self, pre_update: bool = False) -> None:
        # not pre_update: send 1-1 job, then post-update to 1-2.
        # pre_update: no more 0-x job, so pre-update cursor to 1-2, then send 1-1 job
        if not pre_update:
            self.curr_queue_cursor = 1
        elif pre_update and self.queue_order[self.cross_queue_cursor] > 1:
            self.curr_queue_cursor = 2
        else:
            self.curr_queue_cursor = 1

        self.cross_queue_cursor += 1

        if self.cross_queue_cursor >= len(self.queue_order):
            self.cross_queue_cursor = 0

    def _update_queue_cursor(self) -> None:
        logger.debug(
            f"cross_queue_cursor: {self.cross_queue_cursor}, curr_queue_cursor: {self.curr_queue_cursor}"
        )

        self.curr_queue_cursor += 1
        if self.curr_queue_cursor > self.queue_order[self.cross_queue_cursor]:
            self._update_queue_cursor_level()

        logger.debug(
            f"next cross_queue_cursor: {self.cross_queue_cursor}, next curr_queue_cursor: {self.curr_queue_cursor}"
        )

    def _get_queue_level_with_length(self, stage_lists) -> int:
        ori_cross_queue_cursor = self.cross_queue_cursor
        self._update_queue_cursor_level(pre_update=True)

        while len(stage_lists[self.cross_queue_cursor].job_list) == 0:
            self.cross_queue_cursor += 1

            if self.cross_queue_cursor >= len(self.queue_order):
                self.cross_queue_cursor = 0

            # all of the queues are empty
            if self.cross_queue_cursor == ori_cross_queue_cursor:
                logger.warning("No staging Job in all queues")
                break

        logger.debug(
            f"next cross_queue_cursor: {self.cross_queue_cursor}, next curr_queue_cursor: {self.curr_queue_cursor}"
        )
        return self.cross_queue_cursor

    def select_queue(self, stage_lists) -> STAGING_LIST:
        """ common method of queue selector
        """
        next_queue = stage_lists[self.cross_queue_cursor]

        if len(next_queue.job_list) > 0:
            self._update_queue_cursor()
            return next_queue

        # the ori pick queue is empty, choose next one
        logger.warning(
            f"No job in pick queue Level: {next_queue.level}, pick another queue"
        )
        next_queue_level = self._get_queue_level_with_length(stage_lists)
        return stage_lists[next_queue_level]


def get_queue_selector():
    """ Organize the selectors
        select a queue selector based on .env
    """
    selector_map = {
        "top_level_select": TopLevelQueueSelector(),
        "env_weight_random_select": EnvWeightRandomSelect(),
        # "weight_random_select": WeightRandomSelect(),
        "env_zip_select": EnvZipSelect(),
    }

    return selector_map[QUEUE_SELECTION_CONFIG["QUEUE_SELECT_METHOD"]]
