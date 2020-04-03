import time
import logging

from motiv.actor.process import Ticker
from motiv.streams import Emitter, Subscriber


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


class PublisherTicker(Ticker):

    def pre_start(self):
        self.logger = getLogger(self.name)
        self.stream_out.connect()
    def post_stop(self):
        self.stream_out.close()

    def tick(self):
        time.sleep(2)
        self.publish("mytopic", b"Hello world")
        print("\n")
        self.logger.info("\tPublishing to subscribers")


class SubscriberTicker(Ticker):

    def pre_start(self):
        self.logger = getLogger(self.name)
        self.stream_in.subscribe("mytopic")
        self.stream_in.connect()

    def post_stop(self):
        self.stream_in.close()

    def tick(self):
        try:
            channel, payload = self.receive(timeout=3000)
            self.logger.info(f"\tReceived {payload}")
        except TimeoutError:
            self.logger.exception("Timed out")

if __name__ == '__main__':

    pub = PublisherTicker("publisher[1]")
    pub.set_stream(publisher_stream)

    subscribers = []
    for i in range(5):
        subscriber = SubscriberTicker(f"subscriber[{i}]")
        subscriber.set_stream(subscriber_stream)
        subscribers.append(subscriber)

    pub.start()
    [sub.start() for sub in subscribers]
