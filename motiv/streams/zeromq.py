"""Streams implementation using 0MQ

Todo:
    * check address format
"""
import zmq

from ensure import ensure_annotations, ensure
from motiv.streams import mixin
from motiv.channel import Channel, ChannelOut, ChannelIn


class Sender(mixin.SenderType):
    """ZMQ exclusive SenderType"""


class Receiver(mixin.ReceiverType):
    """ZMQ exclusive ReceiverType"""


class Emitter(mixin.EmitterType, Sender):
    """ZMQ Publishing stream

    Args:
        address(str): address to connect to (e.g. /tmp/socket).
        scheme(str): transfer protocol (e.g. ipc, inproc, unix)
    """
    def __init__(self, address: str, scheme: str):
        self.address = address
        cout = ChannelOut(zmq.PUB, scheme, address)
        mixin.EmitterType.__init__(self, cout)

    def publish(self, topic, payload):
        """Publishes data over a topic

        Args:
            topic(int): topic to broadcast payload over.
            payload: data to publish
        """
        _topic = bytes([topic])
        return self.send([_topic, payload])

    def connect(self):
        """establish connection"""
        self.channel_out.bind()


class Subscriber(mixin.SubscriberType, Receiver):

    """ZMQ Subscriber stream

    Args:
        address(str): address to connect to (e.g. /tmp/socket).
        scheme(str): transfer protocol (e.g. ipc, inproc, unix)
    """

    @ensure_annotations
    def __init__(self, address: str, scheme: str):
        self.address = address
        self._cin = ChannelIn(zmq.SUB, scheme, address)

    def subscribe(self, topic: int):
        """Subscribes to a topic

        Args:
            topic(int): topic to subscribe to.
        """
        ensure(topic).is_an(int)
        _topic = bytes([topic])
        self.channel_in.sock_in.setsockopt(zmq.SUBSCRIBE, _topic)

    def connect(self):
        """establish connection"""
        self.channel_in.connect()

    @property
    def channel_in(self):
        """input channel (readonly)"""
        return self._cin


class Ventilator(mixin.VentilatorType, Sender):

    """ZMQ Ventilator

    Args:
        address(str): address to connect to (e.g. /tmp/socket).
        scheme(str): transfer protocol (e.g. ipc, inproc, unix)
    """
    @ensure_annotations
    def __init__(self, address: str, scheme: str):
        self.address = address
        self._cout = ChannelOut(zmq.PUSH, scheme, address)

    def connect(self):
        """establish connection"""
        return self.channel_out.bind()

    @property
    def channel_out(self):
        """output channel (readonly)"""
        return self._cout


class Worker(mixin.WorkerType, Receiver):
    """ZMQ Worker

    Args:
        address(str): address to connect to (e.g. /tmp/socket).
        scheme(str): transfer protocol (e.g. ipc, inproc, unix)
    """

    @ensure_annotations
    def __init__(self, address: str, scheme: str):
        self.address = address
        self._cin = ChannelIn(zmq.PULL, scheme, address)

    def connect(self):
        """establish connection"""
        return self.channel_in.connect()

    @property
    def channel_in(self):
        """input channel (readonly)"""
        return self._cin


class Sink(mixin.SinkType, Receiver):
    """ZMQ Sink

    Args:
        address(str): address to connect to (e.g. /tmp/socket).
        scheme(str): transfer protocol (e.g. ipc, inproc, unix)
    """

    @ensure_annotations
    def __init__(self, address: str, scheme: str):
        self.address = address
        self._cin = ChannelIn(zmq.PULL, scheme, address)

    def connect(self):
        """establish connection"""
        return self.channel_in.bind()

    @property
    def channe_in(self):
        """input channel (readonly)"""
        return self._cin


class CompoundStream(mixin.CompoundStreamType, Sender, Receiver):
    """A stream consisting of two ZMQ Sreams

    Args:
        stream_in: receiver stream.
        stream_out: sender stream.

    Note:
        stream_in and stream_out can be identical.
    """

    @ensure_annotations
    def __init__(self, stream_in: Receiver, stream_out: Sender):
        super().__init__(stream_in, stream_out)
        self.channel = Channel(stream_in.channel_in, stream_out.channel_out)

    def run(self):
        """Starts a proxy over input and output streams"""
        self.channel.proxy()

    @property
    def channel_in(self):
        """input channel"""
        return self.channel

    @property
    def channel_out(self):
        """output channel"""
        return self.channel


__all__ = [
        'Emitter',
        'Subscriber',
        'Ventilator',
        'Worker',
        'Sink',
        'CompoundStream',
        'Receiver',
        'Sender'
        ]
