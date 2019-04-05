import time
import logging

from random import randint

from motiv.actor import Ticker, Proxy, ActorGroup
from motiv.streams import (Emitter, Subscriber,
        Ventilator, CompoundStream, Worker)


def getLogger(name):
    logger = logging.getLogger(f"motiv:{name}")
    logger.setLevel(logging.INFO)

    handler = logging.StreamHandler()
    formatter = logging.Formatter("[%(asctime)s] %(name)-12s: %(message)s")

    handler.setFormatter(formatter)
    logger.addHandler(handler)

    return logger


publisher_stream = Emitter(address="ipc:///tmp/publisher")
subscriber_stream = Subscriber(address="ipc:///tmp/publisher")
ventilator_stream = Ventilator(address="ipc:///tmp/ventilator")
vent_sub_stream = CompoundStream(subscriber_stream, ventilator_stream)


class PublisherTicker(Ticker):

    def pre_start(self):
        self.logger = getLogger(self.name)
        self.stream_out.connect()

    def post_stop(self):
        self.stream_out.close()

    def tick(self):
        time.sleep(2)
        for i in range(randint(1, 3)):
            self.publish(1, b"Hello world")
        print("\n")
        self.logger.info("\tPublishing to subscribers")


class VentingSubProxy(Proxy):

    def pre_start(self):
        self.stream_in.subscribe(1)
        self.stream.connect()

    def post_stop(self):
        self.stream.close()


class WorkerTicker(Ticker):

    def pre_start(self):
        self.stream_in = Worker(address="ipc:///tmp/ventilator")
        self.logger = getLogger(self.name)
        self.stream_in.connect()

    def post_stop(self):
        self.stream_in.close()

    def tick(self):
        payload = self.receive()
        self.logger.info(f"\tReceived {payload}")


if __name__ == '__main__':

    pub = PublisherTicker("publisher[1]")
    pub.set_stream(publisher_stream)
    proxy = VentingSubProxy("venting_subscriber_proxy")
    proxy.set_stream(vent_sub_stream)
    workers = ActorGroup("workers", 3, WorkerTicker)

    pub.start()
    proxy.start()
    workers.start()
