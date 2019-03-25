
import zmq

from ensure import ensure_annotations, ensure
from motiv.streams import mixin
from motiv.channel import ZMQChannel, ZMQChannelOut, ZMQChannelIn

class ZMQSender(mixin.Sender):
    pass

class ZMQReceiver(mixin.Receiver):
    pass

class ZMQEmitter(mixin.Emitter, ZMQSender):

    def __init__(self, address: str, scheme: str):
        self.address = address
        cout = ZMQChannelOut(zmq.PUB, scheme, address)
        mixin.Emitter.__init__(self, cout)

    def publish(self, topic, payload):
        t = bytes([topic])
        return self.send([t, payload])

    def connect(self):
        self.channel_out.bind()

class ZMQSubscriber(mixin.Subscriber, ZMQReceiver):

    @ensure_annotations
    def __init__(self, address: str, scheme: str):
        self.address = address
        cin = ZMQChannelIn(zmq.SUB, scheme, address)
        mixin.Subscriber.__init__(self, cin)

    def subscribe(self, event_type: int):
        ensure(event_type).is_an(int)
        topic = bytes([event_type])
        self.channel_in.sock_in.setsockopt(zmq.SUBSCRIBE, topic)

    def connect(self):
        self.channel_in.connect()

class ZMQVentilator(mixin.Ventilator, ZMQSender):

    @ensure_annotations
    def __init__(self, address: str, scheme: str):
        self.address = address
        cout = ZMQChannelOut(zmq.PUSH, scheme, address)
        Ventilator.__init__(self, cout)

    def connect(self):
        return self.channel_out.bind()

class ZMQWorker(mixin.Worker, ZMQReceiver):

    @ensure_annotations
    def __init__(self, address: str, scheme: str):
        self.address = address
        cin = ZMQChannelIn(zmq.PULL, scheme, address)
        Worker.__init__(self, cin)

    def connect(self):
        return self.channel_in.connect()

class ZMQSink(mixin.Sink, ZMQReceiver):

    @ensure_annotations
    def __init__(self, address: str, scheme: str):
        self.address = address
        cin = ZMQChannelIn(zmq.PULL, scheme, address)
        Worker.__init__(self, cin)

    def connect(self):
        return self.channel_in.bind()

class ZMQCompoundStream(mixin.CompoundStream, ZMQSender, ZMQReceiver):

    @ensure_annotations
    def __init__(self, stream_in: ZMQReceiver, stream_out: ZMQSender):
        super().__init__(stream_in, stream_out)
        self.channel = ZMQChannel(stream_in.channel_in, stream_out.channel_out)

    def run(self):
        self.channel.proxy()

__all__ = [
        'ZMQEmitter',
        'ZMQSubscriber',
        'ZMQVentilator',
        'ZMQWorker',
        'ZMQSink',
        'ZMQCompoundStream',
        'ZMQReceiver',
        'ZMQSender'
        ]
