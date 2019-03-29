import time
import logging

from motiv.actor.process import Actor
from motiv.streams import Emitter, Subscriber


publisher = Emitter(address="/tmp/publisher", scheme="ipc")
subscriber = Subscriber(address="/tmp/publisher", scheme="ipc")

def getLogger(name):
    logger = logging.getLogger(f"motiv:{name}")
    logger.setLevel(logging.INFO)

    handler = logging.StreamHandler()
    formatter = logging.Formatter("%(name)-12s: %(message)s")

    handler.setFormatter(formatter)
    logger.addHandler(handler)

    return logger

class MyPublisher(Actor):

    def preStart(self):
        self.logger = getLogger(self.name)
        self.stream_out.connect()

    def postStop(self):
        self.stream_out.close()

    def tick(self):
        time.sleep(2)
        ts = int(time.monotonic() * 1e6)
        self.publish(1, b"Hello world")
        print("\n")
        self.logger.info("\tPublishing to subscribers\t(monotonic (us): {:,})".format(ts))

class MySubscriber(Actor):

    def preStart(self):
        self.logger = getLogger(self.name)
        subscriber.subscribe(1)
        self.stream_in.connect()

    def postStop(self):
        self.stream_in.close()

    def tick(self):
        channel, payload = self.receive()
        ts = int(time.monotonic() * 1e6)
        self.logger.info("\tReceived {}\t\t(monotonic (us): {:,})".format(payload, ts))


if __name__ == '__main__':
    make_subscriber = lambda idx: (MySubscriber(f"subscriber[{idx}]") + subscriber)

    pub = MyPublisher("publisher[1]")  + publisher
    subscribers = [make_subscriber(i) for i in range(5)]

    pub.start()
    [sub.start() for sub in subscribers]
