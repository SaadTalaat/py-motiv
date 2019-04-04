"""Abstract classes/mixins for IO channels

Module contains base classes that must be derived by any channel
implementation.
"""

import abc

from motiv.sync import SystemEvent


class ChannelOutType(abc.ABC):
    """Abstract class for output channels"""

    @abc.abstractmethod
    def send(self, body, sync=True):
        "abstract send method"

    @abc.abstractmethod
    def close(self):
        "abstract close method"


class ChannelInType(abc.ABC):
    """Abstract class for input channels"""

    @abc.abstractmethod
    def receive(self):
        "abstract receive method"

    @abc.abstractmethod
    def poll(self, exit_condition: SystemEvent, poll_timeout):
        "abstract poll method"

    @abc.abstractmethod
    def close(self):
        "abstract close method"


class ChannelType(ChannelInType, ChannelOutType):
    """Abstract class for duplex channels"""


__all__ = [
        'ChannelType',
        'ChannelInType',
        'ChannelOutType'
        ]
