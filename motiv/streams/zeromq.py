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
    """
    def __init__(self, address: str):
        self.address = address
        self._cout = ChannelOut(zmq.PUB, address)

    def publish(self, topic, payload, sync=True):
        """Publishes data over a topic

        Args:
            topic(int): topic to broadcast payload over.
            payload: data to publish
        """
        _topic = bytes([topic])
        return self.send([_topic, payload], sync)

    def connect(self):
        """establish connection"""
        self.channel_out.bind()

    @property
    def channel_out(self):
        """output channel (readonly)"""
        return self._cout

class Subscriber(mixin.SubscriberType, Receiver):

    """ZMQ Subscriber stream

    Args:
        address(str): address to connect to (e.g. /tmp/socket).
    """

    @ensure_annotations
    def __init__(self, address: str):
        self.address = address
        self._cin = ChannelIn(zmq.SUB, address)

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
    """
    @ensure_annotations
    def __init__(self, address: str):
        self.address = address
        self._cout = ChannelOut(zmq.PUSH, address)

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
    """

    @ensure_annotations
    def __init__(self, address: str):
        self.address = address
        self._cin = ChannelIn(zmq.PULL, address)

    def connect(self):
        """establish connection"""
        return self.channel_in.connect()

    @property
    def channel_in(self):
        """input channel (readonly)"""
        return self._cin


class Pusher(mixin.PusherType, Sender):
    """ZMQ Pusher

    Args:
        address(str): address to connect to (e.g. /tmp/socket)
    """
    @ensure_annotations
    def __init__(self, address: str):
        self.address = address
        self._cout = ChannelOut(zmq.PUSH, address)

    def connect(self):
        """establish connection"""
        return self.channel_out.connect()

    @property
    def channel_out(self):
        """output channel (readonly)"""
        return self._cout


class Sink(mixin.SinkType, Receiver):
    """ZMQ Sink

    Args:
        address(str): address to connect to (e.g. /tmp/socket).
    """

    @ensure_annotations
    def __init__(self, address: str):
        self.address = address
        self._cin = ChannelIn(zmq.PULL, address)

    def connect(self):
        """establish connection"""
        return self.channel_in.bind()

    @property
    def channel_in(self):
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
        self._stream_in = stream_in
        self._stream_out = stream_out
        self.channel = Channel(stream_in.channel_in, stream_out.channel_out)

    def run(self):
        """Starts a proxy over input and output streams"""
        self.channel.proxy()

    def connect(self):
        self.stream_in.connect()
        self.stream_out.connect()

    @property
    def channel_in(self):
        """input channel"""
        return self.channel

    @property
    def channel_out(self):
        """output channel"""
        return self.channel

    @property
    def stream_in(self):
        """output stream"""
        return self._stream_in

    @property
    def stream_out(self):
        """input stream"""
        return self._stream_out


__all__ = [
        'Emitter',
        'Subscriber',
        'Ventilator',
        'Worker',
        'Pusher',
        'Sink',
        'CompoundStream',
        'Receiver',
        'Sender'
        ]
