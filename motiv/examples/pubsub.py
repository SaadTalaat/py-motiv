import time

from motiv.actor.process import ZMQActor
from motiv.streams.zmq import ZMQEmitter, ZMQSubscriber


publisher = ZMQEmitter(address="/tmp/publisher", scheme="ipc")
subscriber = ZMQSubscriber(address="/tmp/publisher", scheme="ipc")

class MyPublisher(ZMQActor):

    def preStart(self):
        self.stream_out.connect()

    def postStop(self):
        self.stream_out.close()

    def tick(self):
        time.sleep(2)
        self.publish(1, b"Hello world")

class MySubscriber(ZMQActor):

    def preStart(self):
        subscriber.subscribe(1)
        self.stream_in.connect()

    def postStop(self):
        self.stream_in.close()

    def tick(self):
        channel, payload = self.receive()
        print(f"{self.name} Subscriber received {payload}")


pub = MyPublisher("pub1")  + publisher
sub = MySubscriber("sub1")  + subscriber
sub2 = MySubscriber("sub2") + subscriber
pub.start()
sub.start()
sub2.start()
