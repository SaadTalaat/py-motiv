import abc

from ensure import ensure_annotations
from motiv.sync import SystemEvent

class ChannelOut(abc.ABC):

    @abc.abstractmethod
    def send(self, body):
        pass

    @abc.abstractmethod
    def close(self):
        pass

class ChannelIn(abc.ABC):

    @abc.abstractmethod
    def receive(self):
        pass

    @abc.abstractmethod
    def poll(self, exit_condition: SystemEvent, interval):
        pass

    @abc.abstractmethod
    def close(self):
        pass

class Channel(ChannelIn, ChannelOut):
    pass

__all__ = [
        'Channel', 'ChannelIn', 'ChannelOut'
        ]
