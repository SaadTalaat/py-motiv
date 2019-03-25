import os
import zmq
from ensure import ensure_annotations, ensure

from motiv.exceptions import AlreadyConnected, NotConnected
from motiv.sync import SystemEvent
from motiv.channel.mixin import Channel, ChannelIn, ChannelOut
from motiv.proto.zmq import create_socket


class ZMQChannelOut(ChannelOut):
    """
    A sending only channel:
        out-channels bind to an address.
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
        if self.sock_connected:
            raise AlreadyConnected("channel already initialized and connected")
        self.sock_out.bind(self.address_out)
        self.sock_connected = True

    def connect(self):
        if self.sock_connected:
            raise AlreadyConnected("channel already initialized and connected")
        self.sock_out.connect(self.address_out)
        self.sock_connected = True

    def send_multipart(self, body: list):
        return self.sock_out.send_multipart(body)

    def send(self, body):
        if not self.sock_connected:
            raise NotConnected("channel has not binded nor connected")

        if isinstance(body, bytes):
            return self.send_multipart([body])
        elif isinstance(body, (list, tuple)):
            return self.send_multipart(body)
        else:
            raise TypeError("body is not a buffer type (bytes, list, tuple)")

    def close(self):
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


class ZMQChannelIn(ChannelIn):
    """
    A sending only channel:
        out-channels bind to an address.
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
        if self.sock_connected:
            raise AlreadyConnected("channel already initialized and connected")
        self.sock_in.bind(self.address_in)
        self.sock_connected = True

    def connect(self):
        if self.sock_connected:
            raise AlreadyConnected("channel already initialized and connected")
        self.sock_in.connect(self.address_in)
        self.sock_connected = True

    def receive(self):
        if not self.sock_connected:
            raise NotConnected("channel has not binded nor connected")
        return self.sock_in.recv_multipart()

    def poll(self, poller, exit_condition: SystemEvent, poll_interval=50):
        if not self.sock_connected:
            raise NotConnected("channel has not binded nor connected")
        ensure(exit_condition).is_a(SystemEvent)
        if self.sock_in not in poller:
            poller.register(self)

        while not exit_condition.is_set():
            socks = dict(poller.poll(poll_interval))
            if self.sock_in in socks:
                return self.receive()

        return None

    def close(self):
        self.sock_in.close()

    @property
    def sock_in(self):
        if not self._sock_in:
            ctx = zmq.Context(2)
            self._sock_in = create_socket(ctx, self.sock_type)
        return self._sock_in


class ZMQChannel(Channel):

    @ensure_annotations
    def __init__(self, channel_in: ZMQChannelIn, channel_out: ZMQChannelOut):
        self.cin = channel_in
        self.cout = channel_out

    def proxy(self):
        if not(self.cin.sock_connected and self.cout.sock_connected):
            raise NotConnected("channels have not binded nor connected")
        ensure(self.cin.address_in).is_not_equal_to(self.cout.address_out)
        zmq.proxy(self.cin.sock_in, self.cout.sock_out)
        self.close()

    def send(self, body):
        return self.cout.send(body)

    def receive(self):
        return self.cin.receive()

    def poll(self, exit_condition, poll_interval=50):
        return self.cin.poll(exit_condition, poll_interval)

    def close(self):
        self.cin.close()
        self.cout.close()

class ZMQPoller(zmq.Poller):

    @ensure_annotations
    def register(self, channel: (ZMQChannelIn, ZMQChannelOut)):

        if isinstance(channel, ZMQChannelIn):
            return super().register(channel.sock_in, zmq.POLLIN)
        elif isinstance(channel, ZMQChannelOut):
            return super().register(channel.sock_out, zmq.POLLOUT)
        else:
            raise RuntimeError("Unknown error")

    @ensure_annotations
    def unregister(self, channel: (ZMQChannelIn, ZMQChannelOut)):
        if isinstance(channel, ZMQChannelIn):
            return super().unregister(channel.sock_in)
        elif isinstance(channel, ZMQChannelOut):
            return super().unregister(channel.sock_out)
        else:
            raise RuntimeError("Unknown error")


__all__ = ['ZMQChannel', 'ZMQChannelIn', 'ZMQChannelOut', 'ZMQPoller']
