
import zmq

from ensure import ensure_annotations, ensure
from motiv.streams import mixin
from motiv.channel import Channel, ChannelOut, ChannelIn

class Sender(mixin.SenderType):
    pass

class Receiver(mixin.ReceiverType):
    pass

class Emitter(mixin.EmitterType, Sender):

    def __init__(self, address: str, scheme: str):
        self.address = address
        cout = ChannelOut(zmq.PUB, scheme, address)
        mixin.EmitterType.__init__(self, cout)

    def publish(self, topic, payload):
        t = bytes([topic])
        return self.send([t, payload])

    def connect(self):
        self.channel_out.bind()

class Subscriber(mixin.SubscriberType, Receiver):

    @ensure_annotations
    def __init__(self, address: str, scheme: str):
        self.address = address
        cin = ChannelIn(zmq.SUB, scheme, address)
        mixin.SubscriberType.__init__(self, cin)

    def subscribe(self, event_type: int):
        ensure(event_type).is_an(int)
        topic = bytes([event_type])
        self.channel_in.sock_in.setsockopt(zmq.SUBSCRIBE, topic)

    def connect(self):
        self.channel_in.connect()

class Ventilator(mixin.VentilatorType, Sender):

    @ensure_annotations
    def __init__(self, address: str, scheme: str):
        self.address = address
        cout = ChannelOut(zmq.PUSH, scheme, address)
        mixin.VentilatorType.__init__(self, cout)

    def connect(self):
        return self.channel_out.bind()

class Worker(mixin.WorkerType, Receiver):

    @ensure_annotations
    def __init__(self, address: str, scheme: str):
        self.address = address
        cin = ChannelIn(zmq.PULL, scheme, address)
        mixin.WorkerType.__init__(self, cin)

    def connect(self):
        return self.channel_in.connect()

class Sink(mixin.SinkType, Receiver):

    @ensure_annotations
    def __init__(self, address: str, scheme: str):
        self.address = address
        cin = ChannelIn(zmq.PULL, scheme, address)
        mixin.SinkType.__init__(self, cin)

    def connect(self):
        return self.channel_in.bind()

class CompoundStream(mixin.CompoundStreamType, Sender, Receiver):

    @ensure_annotations
    def __init__(self, stream_in: Receiver, stream_out: Sender):
        super().__init__(stream_in, stream_out)
        self.channel = Channel(stream_in.channel_in, stream_out.channel_out)

    def run(self):
        self.channel.proxy()

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
