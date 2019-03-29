
import abc
from ensure import ensure_annotations, ensure
from motiv.channel import ChannelOutType, ChannelInType


class SenderType(abc.ABC):

    @abc.abstractmethod
    def send(self, payload):
        pass

class ReceiverType(abc.ABC):

    @abc.abstractmethod
    def receive(self):
        pass

class EmitterType(SenderType):

    def __init__(self, channel_out: ChannelOutType):
        ensure(channel_out).is_a(ChannelOutType)
        self.channel_out = channel_out

    @abc.abstractmethod
    def connect(self):
        pass

    def send(self, event):
        self.channel_out.send(event)

    def close(self):
        return self.channel_out.close()

class SubscriberType(ReceiverType):

    def __init__(self, channel_in: ChannelInType):
        ensure(channel_in).is_a(ChannelInType)
        self.channel_in = channel_in

    @abc.abstractmethod
    def connect(self):
        pass

    @abc.abstractmethod
    def subscribe(self, event_type: int):
        pass

    def receive(self):
        return self.channel_in.receive()

    def poll(self, *args, **kwargs):
        return self.channel_in.poll(*args, **kwargs)

    def close(self):
        return self.channel_in.close()

class VentilatorType(SenderType):

    def __init__(self, channel_out: ChannelOutType):
        ensure(channel_out).is_a(ChannelOutType)
        self.channel_out = channel_out

    @abc.abstractmethod
    def connect(self):
        pass

    def send(self, body):
        self.channel_out.send(body)

    def close(self):
        return self.channel_out.close()

class WorkerType(ReceiverType):

    def __init__(self, channel_in: ChannelInType):
        ensure(channel_in).is_a(ChannelInType)
        self.channel_in = channel_in

    @abc.abstractmethod
    def connect(self):
        pass

    def receive(self):
        return self.channel_in.receive()

    def poll(self, *args, **kwargs):
        return self.channel_in.poll(*args, **kwargs)

    def close(self):
        return self.channel_in.close()


class SinkType(ReceiverType):

    def __init__(self, channel_in: ChannelInType):
        ensure(channel_in).is_a(ChannelInType)
        self.channel_in = channel_in

    @abc.abstractmethod
    def connect(self):
        pass

    def receive(self):
        return self.channel_in.receive()

    def poll(self, *args, **kwargs):
        return self.channel_in.poll(*args, **kwargs)

    def close(self):
        return self.channel_in.close()

class CompoundStreamType(SenderType, ReceiverType):

    def __init__(self, stream_in: ReceiverType, stream_out: SenderType):
        ensure(stream_in).is_a(ReceiverType)
        ensure(stream_out).is_a(SenderType)
        self.stream_in = stream_in
        self.stream_out = stream_out

    def send(self, payload):
        self.stream_out.send(payload)

    def receive(self):
        self.stream_in.receive()

    def poll(self, *args, **kwargs):
        self.stream_in.poll(*args, **kwargs)

    def close(self):
        self.stream_in.close()
        self.stream_out.close()


__all__ = [
        'EmitterType',
        'SubscriberType',
        'VentilatorType',
        'WorkerType',
        'SinkType',
        'CompoundStreamType',
        ]
