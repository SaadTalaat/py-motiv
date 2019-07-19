import time
import random
import logging
import pickle

from motiv.serde import Serializable
from motiv.actor import Ticker, ActorGroup
from motiv.streams import (Ventilator, Worker,
        Pusher, Sink, CompoundStream)


def getLogger(name):
    logger = logging.getLogger(f"motiv:{name}")
    logger.setLevel(logging.INFO)

    handler = logging.StreamHandler()
    formatter = logging.Formatter("[%(asctime)s] %(name)-12s: %(message)s")

    handler.setFormatter(formatter)
    logger.addHandler(handler)

    return logger


vent_stream = Ventilator(address="ipc:///tmp/ventilator")
sink_stream = Sink(address="ipc:///tmp/sink")


class WorkResult(Serializable):

    def __init__(self, worker_id, result):
        self.worker_id = worker_id
        self.result = result

    def __repr__(self):
        return f"WorkerResult[worker_id={self.worker_id},"\
               f" result={self.result}]"

    def serialize(self):
        return pickle.dumps(self)

    @classmethod
    def deserialize(cls, pickled):
        return pickle.loads(pickled)


class VentilatorTicker(Ticker):

    def pre_start(self):
        self.logger = getLogger(self.name)
        self.stream_out.connect()

    def post_stop(self):
        self.logger.info("Stopping..")
        self.stream_out.close()

    def tick(self):
        time.sleep(2)
        batch_cnt = random.randint(1, 10)
        for i in range(batch_cnt):
            self.send(f"Work batch [{i}]".encode("utf-8"))

        print("\n")
        self.logger.info(f"\t{batch_cnt} work batches sent")


class WorkerTicker(Ticker):

    def pre_start(self):
        worker_stream = Worker(address="ipc:///tmp/ventilator")
        pusher_stream = Pusher(address="ipc:///tmp/sink")
        self.stream = CompoundStream(worker_stream, pusher_stream)

        self.logger = getLogger(self.name)
        self.stream.connect()

    def post_stop(self):
        self.logger.info("Stopping..")
        self.stream.close()

    def tick(self):
        try:
            payload = self.receive(timeout=3000)
            self.logger.info(f"\tReceived {payload}")
            result = WorkResult(self.name, payload)
            self.send(result, sync=False)
        except TimeoutError:
            self.logger.exception("Timed out")


class SinkTicker(Ticker):
    def pre_start(self):
        self.logger = getLogger(self.name)
        self.stream_in.connect()

    def post_stop(self):
        self.logger.info("Stopping")
        self.stream_in.close()

    def tick(self):
        try:
            payload = self.receive()
            for frame in payload:
                result = WorkResult.deserialize(frame)
                self.logger.info(f"\tReceived {result}")
        except TimeoutError:
            self.logger.exception("Timed out")


if __name__ == '__main__':

    vent = VentilatorTicker("ventilator[0]")
    vent.set_stream(vent_stream)
    sink = SinkTicker("sink[0]")
    sink.set_stream(sink_stream)
    workers = ActorGroup("workers", 3, WorkerTicker)

    vent.start()
    sink.start()
    workers.start()
