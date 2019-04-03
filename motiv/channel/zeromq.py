"""Channels implementation using 0MQ

Todo:
    * Check sockaddr format
"""

import os
import zmq
from ensure import ensure_annotations, ensure

from motiv.exceptions import AlreadyConnected, NotConnected
from motiv.sync import SystemEvent
from motiv.serde import Serializable
from motiv.channel.mixin import ChannelType, ChannelInType, ChannelOutType
from motiv.proto.zeromq import create_socket


class ChannelOut(ChannelOutType):
    """Sending only output channel

    0MQ implementation for `motiv.channel.mixin.ChannelOutType`.
    An output channel can either bind or connect to the address
    given.

    Args:
        sock_type (int): 0MQ socket_type (e.g. `zmq.PULL`)
        scheme (str): transfer protocol
        sockaddr (str): address to connect/bind to.

    Todo:
        * ensure sock_type is indeed a zmq socket type

    """

    @ensure_annotations
    def __init__(self, sock_type: int, scheme: str, sockaddr: str):
        # Internal only communication.
        ensure(scheme).is_in(['inproc', 'ipc', 'unix'])
        self.address_out = f"{scheme}://{sockaddr}"
        self.sock_connected = False
        self.sock_type = sock_type
        self.pid = os.getpid()
        self._sock_out = None

    def bind(self):
        """zmq socket binds to channel's address"""
        if self.sock_connected:
            raise AlreadyConnected("channel already initialized and connected")
        self.sock_out.bind(self.address_out)
        self.sock_connected = True

    def connect(self):
        """zmq socket connects to channel's address"""
        if self.sock_connected:
            raise AlreadyConnected("channel already initialized and connected")
        self.sock_out.connect(self.address_out)
        self.sock_connected = True

    def _send_multipart(self, body: list):
        return self.sock_out.send_multipart(body)

    def send(self, body):
        """sends streamed payload over the channel.

        Args:
            body (bytes, list, tuple): the payload or a list of payloads.
        """
        if not self.sock_connected:
            raise NotConnected("channel has not binded nor connected")

        result = None
        if isinstance(body, bytes):
            result = self._send_multipart([body])
        elif isinstance(body, Serializable):
            payload = body.serialize()
            result = self._send_multipart([payload])
        elif isinstance(body, (list, tuple)):
            frames = []
            for frame in body:
                if isinstance(frame, bytes):
                    frames.append(frame)
                elif isinstance(frame, Serializable):
                    frames.append(frame.serialize())
                else:
                    raise TypeError("Frames must be of type bytes"
                                    " or a serializable class")
            result = self._send_multipart(frames)
        else:
            raise TypeError("body is not a buffer type (bytes, list, tuple)")
        return result

    def close(self):
        """closes channel"""
        self.sock_out.close()

    @property
    def sock_out(self):
        """
        Lazily evaluated
        """
        if self._sock_out is None:
            ctx = zmq.Context(2)
            self._sock_out = create_socket(ctx, self.sock_type)
        return self._sock_out


class ChannelIn(ChannelInType):
    """Receiving only input channel.

    0MQ implementation for `motiv.channel.mixin.ChannelInType`.
    An input channel can either bind or connect to the address
    given.

    Args:
        sock_type (int): 0MQ socket_type (e.g. `zmq.PULL`)
        scheme (str): transfer protocol
        sockaddr (str): address to connect/bind to.

    Todo:
        * ensure sock_type is indeed a zmq socket type

    """

    @ensure_annotations
    def __init__(self, sock_type: int, scheme: str, sockaddr: str):
        # Internal only communication.
        ensure(scheme).is_in(['inproc', 'ipc', 'unix'])
        self.address_in = f"{scheme}://{sockaddr}"
        self.pid = os.getpid()
        self.sock_connected = False
        self.sock_type = sock_type
        self._sock_in = None

    def bind(self):
        """zmq socket binds to channel's address"""
        if self.sock_connected:
            raise AlreadyConnected("channel already initialized and connected")
        self.sock_in.bind(self.address_in)
        self.sock_connected = True

    def connect(self):
        """zmq socket connects to channel's address"""
        if self.sock_connected:
            raise AlreadyConnected("channel already initialized and connected")
        self.sock_in.connect(self.address_in)
        self.sock_connected = True

    def receive(self):
        """receives data over the channel
        Note:
            this is a blocking method
        """
        if not self.sock_connected:
            raise NotConnected("channel has not binded nor connected")
        return self.sock_in.recv_multipart()

    def poll(self, poller, exit_condition: SystemEvent, poll_timeout=50):
        """polls input socket for data received

        Note:
            this method returns as soon as it receives data or
            halt event is set.

        Args:
            poller: normally `motiv.channel.zeromq.Poller`.
            exit_condition: halt event to set to give back control to caller.
            poll_timeout: how long to wait for messages on each iteration.

        """

        if not self.sock_connected:
            raise NotConnected("channel has not binded nor connected")
        ensure(exit_condition).is_a(SystemEvent)
        if self.sock_in not in poller:
            poller.register_channel(self)

        while not exit_condition.is_set():
            socks = dict(poller.poll(poll_timeout))
            if self.sock_in in socks:
                return self.receive()

    def close(self):
        """closes channel"""
        self.sock_in.close()

    @property
    def sock_in(self):
        """channel's underlying input socket"""
        if not self._sock_in:
            ctx = zmq.Context(2)
            self._sock_in = create_socket(ctx, self.sock_type)
        return self._sock_in


class Channel(ChannelType):
    """Duplex channel

    0MQ implementation for `motiv.channel.mixin.ChannelType`.
    A duplex channel contains two underlying streams, input
    and output stream. these two streams can be the same stream
    or distinct streams.

    Args:
        channel_in: Input channel
        channel_out: Output channel
    """

    @ensure_annotations
    def __init__(self, channel_in: ChannelIn, channel_out: ChannelOut):
        self.cin = channel_in
        self.cout = channel_out

    def proxy(self):
        """Starts a forwarding proxy between input and output channels"""

        if not(self.cin.sock_connected and self.cout.sock_connected):
            raise NotConnected("channels have not binded nor connected")
        ensure(self.cin.address_in).is_not_equal_to(self.cout.address_out)
        zmq.proxy(self.cin.sock_in, self.cout.sock_out)
        self.close()

    def send(self, body):
        """Sends a payload over the output channel"""
        return self.cout.send(body)

    def receive(self):
        """blocks to receive data over input channel"""
        return self.cin.receive()

    def poll(self, poller, exit_condition, poll_timeout=50):
        """polls input channel for received data"""
        return self.cin.poll(poller, exit_condition, poll_timeout=50)

    def close(self):
        """closes input, output channels"""
        self.cin.close()
        self.cout.close()


class Poller(zmq.Poller):
    """Wrapper for `zmq.Poller`"""

    @ensure_annotations
    def register_channel(self, channel: (ChannelIn, ChannelOut)):
        """registers a channel to the poller
        Args:
            channel: the channel to register.
        """
        if isinstance(channel, ChannelIn):
            self.register(channel.sock_in, zmq.POLLIN)
        if isinstance(channel, ChannelOut):
            self.register(channel.sock_out, zmq.POLLOUT)

    @ensure_annotations
    def unregister_channel(self, channel: (ChannelIn, ChannelOut)):
        """unregisters a channel from the poller

        Args:
            channel: channel to unregister
        """
        if isinstance(channel, ChannelIn):
            self.unregister(channel.sock_in)
        if isinstance(channel, ChannelOut):
            self.unregister(channel.sock_out)


__all__ = [
        'Channel',
        'ChannelIn',
        'ChannelOut',
        'Poller']
