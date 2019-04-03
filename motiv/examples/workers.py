import time
import random
import logging

from motiv.actor.process import Ticker
from motiv.streams import Ventilator, Worker


def getLogger(name):
    logger = logging.getLogger(f"motiv:{name}")
    logger.setLevel(logging.INFO)

    handler = logging.StreamHandler()
    formatter = logging.Formatter("[%(asctime)s] %(name)-12s: %(message)s")

    handler.setFormatter(formatter)
    logger.addHandler(handler)

    return logger


vent_stream = Ventilator(address="/tmp/ventilator", scheme="ipc")
worker_stream = Worker(address="/tmp/ventilator", scheme="ipc")


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
        self.logger = getLogger(self.name)
        self.stream_in.connect()

    def post_stop(self):
        self.logger.info("Stopping..")
        self.stream_in.close()

    def tick(self):
        payload = self.receive()
        self.logger.info(f"\tReceived {payload}")


if __name__ == '__main__':

    vent = VentilatorTicker("ventilator[0]")
    vent.set_stream(vent_stream)

    workers = []
    for i in range(3):
        worker = WorkerTicker(f"worker[{i}]")
        worker.set_stream(worker_stream)
        workers.append(worker)

    vent.start()
    [w.start() for w in workers]
