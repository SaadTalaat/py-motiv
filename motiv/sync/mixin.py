
import abc


class SystemEvent(abc.ABC):

    @abc.abstractmethod
    def is_set(self):
        return

    @abc.abstractmethod
    def set(self):
        return

    @abc.abstractmethod
    def clear(self):
        return

__all__ = ['SystemEvent']
