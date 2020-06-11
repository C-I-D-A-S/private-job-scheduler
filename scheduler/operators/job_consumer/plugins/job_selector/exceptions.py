class EmptyListException(Exception):
    """ The current staging List is empty

    """

    def __str__(self):
        return "Queue is Empty"


class NoValidJobInListException(Exception):
    """ There is no valid job in the current staging List

    """

    def __init__(self, system_resources, message="no valid job"):
        self.system_resources = system_resources
        self.message = message
        super().__init__(self.message)

    def __str__(self):
        return f"Queue NO VALID JOB - SYSTEM: {self.system_resources}"


class NoValidJobInAllListException(Exception):
    """ There is no valid job in all of current staging lists

    """

    pass
