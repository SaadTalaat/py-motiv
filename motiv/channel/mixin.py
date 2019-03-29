"""Abstract classes/mixins for IO channels

Module contains base classes that must be derived by any channel implementation.
"""

import abc

from ensure import ensure_annotations
from motiv.sync import SystemEvent

class ChannelOutType(abc.ABC):
    """Abstract class for output channels"""

    @abc.abstractmethod
    def send(self, body):
        pass

    @abc.abstractmethod
    def close(self):
        pass

class ChannelInType(abc.ABC):
    """Abstract class for input channels"""

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
    """Abstract class for duplex channels"""
    pass

__all__ = [
        'ChannelType', 'ChannelInType', 'ChannelOutType'
        ]
