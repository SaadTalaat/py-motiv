import abc

from ensure import ensure_annotations
from motiv.sync import SystemEvent

class ChannelOutType(abc.ABC):

    @abc.abstractmethod
    def send(self, body):
        pass

    @abc.abstractmethod
    def close(self):
        pass

class ChannelInType(abc.ABC):

    @abc.abstractmethod
    def receive(self):
        pass

    @abc.abstractmethod
    def poll(self, exit_condition: SystemEvent, interval):
        pass

    @abc.abstractmethod
    def close(self):
        pass

class ChannelType(ChannelInType, ChannelOutType):
    pass

__all__ = [
        'ChannelType', 'ChannelInType', 'ChannelOutType'
        ]
