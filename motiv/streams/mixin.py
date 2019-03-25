
import abc
from ensure import ensure_annotations, ensure
from motiv.channel import Channel, ChannelOut, ChannelIn


class Sender(abc.ABC):

    @abc.abstractmethod
    def send(self, payload):
        pass

class Receiver(abc.ABC):

    @abc.abstractmethod
    def receive(self):
        pass

class Emitter(Sender):

    def __init__(self, channel_out: ChannelOut):
        ensure(channel_out).is_a(ChannelOut)
        self.channel_out = channel_out

    @abc.abstractmethod
    def connect(self):
        pass

    def send(self, event):
        self.channel_out.send(event)

    def close(self):
        return self.channel_out.close()

class Subscriber(Receiver):

    def __init__(self, channel_in: ChannelIn):
        ensure(channel_in).is_a(ChannelIn)
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

class Ventilator(Sender):

    def __init__(self, channel_out: ChannelOut):
        ensure(channel_out).is_a(ChannelOut)
        self.channel_out = channel_out

    @abc.abstractmethod
    def connect(self):
        pass

    def send(self, body):
        self.channel_out.send(body)

    def close(self):
        return self.channel_out.close()

class Worker(Receiver):

    def __init__(self, channel_in: ChannelIn):
        ensure(channel_in).is_a(ChannelIn)
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


class Sink(Receiver):

    def __init__(self, channel_in: ChannelIn):
        ensure(channel_in).is_a(ChannelIn)
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

class CompoundStream(Sender, Receiver):

    def __init__(self, stream_in: Receiver, stream_out: Sender):
        ensure(stream_in).is_a(Receiver)
        ensure(stream_out).is_a(Sender)
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
        'Emitter',
        'Subscriber',
        'Ventilator',
        'Worker',
        'Sink',
        'CompoundStream',
        ]
