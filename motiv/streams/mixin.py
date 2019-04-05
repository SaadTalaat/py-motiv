"""Streams interfaces and abstract classess

Module contains base classes for stream patterns that
must be derived by any stream implementation
"""

import abc


class SenderType(abc.ABC):
    """Sender stream type"""
    @abc.abstractmethod
    def send(self, payload, sync):
        """abstract send method"""


class ReceiverType(abc.ABC):
    """Receiver stream type"""
    @abc.abstractmethod
    def receive(self):
        """abstract receive method"""


class EmitterType(SenderType):
    """Emitter stream abstract class"""

    @abc.abstractmethod
    def connect(self):
        """abstract connect method"""

    def send(self, payload, sync=True):
        """sends a payload over underlying channel

        Args:
            payload: data to send over the channel
        """
        return self.channel_out.send(payload, sync)

    def close(self):
        """closes underlying channel"""
        return self.channel_out.close()

    @property
    @abc.abstractmethod
    def channel_out(self):
        """output channel"""


class SubscriberType(ReceiverType):
    """Subscriber stream abstract class"""

    @abc.abstractmethod
    def connect(self):
        """abstract connect method"""

    @abc.abstractmethod
    def subscribe(self, topic: int):
        """subscribe to a topic

        Args:
            topic (int): topic to subscribe to.
        """

    def receive(self):
        """block on the input channel for received data"""
        return self.channel_in.receive()

    def poll(self, *args, **kwargs):
        """polls input channel for received data"""
        return self.channel_in.poll(*args, **kwargs)

    def close(self):
        """closes stream's underlying channel"""
        return self.channel_in.close()

    @property
    @abc.abstractmethod
    def channel_in(self):
        """input channel"""


class VentilatorType(SenderType):
    """Ventilator stream abstract class"""

    @abc.abstractmethod
    def connect(self):
        """abstract connect method"""

    def send(self, body, sync=True):
        """sends a payload over underlying channel

        Args:
            payload: data to send over the channel
        """
        self.channel_out.send(body, sync)

    def close(self):
        """closes underlying channel"""
        return self.channel_out.close()

    @property
    @abc.abstractmethod
    def channel_out(self):
        """output channel"""


class WorkerType(ReceiverType):
    """Worker stream abstract class"""

    @abc.abstractmethod
    def connect(self):
        """abstract connect method"""

    def receive(self):
        """block on the input channel for received data"""
        return self.channel_in.receive()

    def poll(self, *args, **kwargs):
        """polls input channel for received data"""
        return self.channel_in.poll(*args, **kwargs)

    def close(self):
        """closes underlying channel"""
        return self.channel_in.close()

    @property
    @abc.abstractmethod
    def channel_in(self):
        """input channel"""

class PusherType(SenderType):
    """pushes data into a pulling stream (sink)"""

    @abc.abstractmethod
    def connect(self):
        """abstract connect method"""

    def send(self, body, sync=True):
        """sends a payload over underlying channel

        Args:
            payload: data to send over the channel
        """
        self.channel_out.send(body, sync)

    def close(self):
        """closes underlying channel"""
        return self.channel_out.close()

    @property
    @abc.abstractmethod
    def channel_out(self):
        """output channel"""


class SinkType(ReceiverType):
    """Sink stream abstract class"""

    @abc.abstractmethod
    def connect(self):
        """abstract connect method"""

    def receive(self):
        """block on the input channel for received data"""
        return self.channel_in.receive()

    def poll(self, *args, **kwargs):
        """polls input channel for received data"""
        return self.channel_in.poll(*args, **kwargs)

    def close(self):
        """closes underlying channel"""
        return self.channel_in.close()

    @property
    @abc.abstractmethod
    def channel_in(self):
        """input channel"""


class CompoundStreamType(SenderType, ReceiverType):
    """A type of two merged streams, ideally sender and receiver stream"""
    def send(self, payload, sync=True):
        """sends a payload over underlying channel

        Args:
            payload: data to send over the channel
        """
        self.stream_out.send(payload, sync)

    def receive(self):
        """block on the input channel for received data"""
        self.stream_in.receive()

    def poll(self, *args, **kwargs):
        """polls input channel for received data"""
        self.stream_in.poll(*args, **kwargs)

    @abc.abstractmethod
    def connect(self):
        """establish connection"""

    def close(self):
        """closes underlying streams"""
        self.stream_in.close()
        self.stream_out.close()


__all__ = [
        'EmitterType',
        'SubscriberType',
        'VentilatorType',
        'WorkerType',
        'PusherType',
        'SinkType',
        'CompoundStreamType',
        ]
