"""motiv synchronization primitives

Module:
    Contains different types of internally used
    events
"""

from multiprocessing import Event as PEvent
from threading import Timer, Event as TEvent
from motiv.sync import mixin


class ProcessEvent(mixin.SystemEvent):

    """Event synchronization primitive.
    Wraps `multiprocessing.Event`
    """
    def __init__(self, *args, **kwargs):
        self.event = PEvent(*args, **kwargs)

    def set(self):
        """sets the event"""
        return self.event.set()

    def clear(self):
        """clears the event"""
        return self.event.clear()

    def wait(self, *args, **kwargs):
        """waits till event is set"""
        return self.event.wait(*args, **kwargs)

    def is_set(self):
        """checks if the event is set

        Returns:
            bool: True if set, False if cleared
        """
        return self.event.is_set()


class ThreadEvent(mixin.SystemEvent):

    """Event synchronization primitive.
    Wraps `threading.Event`
    """
    def __init__(self, *args, **kwargs):
        self.event = TEvent(*args, **kwargs)

    def set(self):
        """sets the event"""
        return self.event.set()

    def clear(self):
        """clears the event"""
        return self.event.clear()

    def wait(self, *args, **kwargs):
        """waits till event is set"""
        return self.event.wait(*args, **kwargs)

    def is_set(self):
        """checks if the event is set

        Returns:
            bool: True if set, False if cleared
        """
        return self.event.is_set()


class TimeoutEvent(ThreadEvent):

    """Timeout event that is automatically set
    after a specified timeout
    Args:
        timeout(int): timeout in millis

    """

    def __init__(self, timeout, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.timer = Timer(timeout * 1e-3, self.set)

    def start(self):
        """Starts the timer for the timeout"""
        return self.timer.start()


__all__ = [
        'ProcessEvent',
        'ThreadEvent',
        'TimeoutEvent'
        ]
