"""motiv synchronization primitives

Module:
    Using a uniform interface to define synchronization
    primitives helps us use multiple execution frameworks
    without changing any of the code written.
    for example, multiprocessing vs threading.
"""

import abc


class SystemEvent(abc.ABC):
    """Event abstract class"""

    @abc.abstractmethod
    def is_set(self):
        """checks if the event is set."""

    @abc.abstractmethod
    def set(self):
        """sets the event"""

    @abc.abstractmethod
    def clear(self):
        """clears the event"""

    @abc.abstractmethod
    def wait(self, *args, **kwargs):
        """waits till event is set"""


__all__ = [
        'SystemEvent',
        ]
