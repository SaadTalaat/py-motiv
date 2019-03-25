
import abc
from ensure import ensure_annotations, ensure
from multiprocessing import Process, Event

from motiv.sync import SystemEvent
from motiv.exceptions import ActorInitializationError
from motiv.channel import ZMQPoller
from motiv.streams.zmq import ZMQSender, ZMQReceiver, ZMQEmitter


class ProcessEvent(SystemEvent):

    def __init__(self, *args, **kwargs):
        self.event = Event(*args, **kwargs)

    def set(self):
        return self.event.set()

    def clear(self):
        return self.event.clear()

    def is_set(self):
        return self.event.is_set()

class ActorProcessBase(Process, abc.ABC):

    def __init__(self, name: str):
        self.name = name
        self._halt = ProcessEvent()
        super().__init__(name=name)

    @abc.abstractmethod
    def preStart(self):
        pass

    @abc.abstractmethod
    def postStop(self):
        pass

    @abc.abstractmethod
    def tick(self):
        pass


    def run(self):
        self.preStart()
        while self.runnable:
            self.tick()

        self.postStop()

    def stop(self):
        return self._halt.set()

    @property
    def runnable(self):
        return not self._halt.is_set()


class ZMQActor(ActorProcessBase):

    def __init__(self, name):
        self.name = name
        super().__init__(self.name)
        self._stream_in = None
        self._stream_out = None
        self._stream = None
        self.poller = ZMQPoller()

    def proxy(self):
        if not(self.stream or (self.stream_in and self.stream_out)):
            raise ActorInitializationError("No streams set")
        self.stream = self.stream or ZMQCompoundStream(self.stream_in, self.stream_out)
        return self.stream.run()

    def receive(self):
        if not self.stream_in:
            raise ActorInitializationError("No in-stream set")
        return self.stream_in.poll(self.poller, self._halt)

    def send(self, payload):
        if not self.stream_out:
            raise ActorInitializationError("No out-stream set")
        return self.stream_out.send(payload)

    def publish(self, topic, payload):
        ensure(self.stream_out).is_a(ZMQEmitter)
        return self.stream_out.publish(topic, payload)

    def __add__(self, other):
        if isinstance(other, ZMQSender) and isinstance(other, ZMQReceiver):
            self.stream = other
            return self
        elif isinstance(other, ZMQSender):
            self.stream_out = other
            return self
        elif isinstance(other, ZMQReceiver):
            self.stream_in = other
            return self
        else:
            raise ValueError("Incompatible stream type")

    @property
    def stream_in(self):
        return self._stream_in

    @stream_in.setter
    @ensure_annotations
    def stream_in(self, value: ZMQReceiver):
        if self.stream is not None:
            raise ValueError("stream_in is set from stream, reset stream to overwrite")
        self._stream_in = value

    @property
    def stream_out(self):
        return self._stream_out

    @stream_out.setter
    def stream_out(self, value: ZMQSender):
        if self.stream is not None:
            raise ValueError("stream_out is set from stream, reset stream to overwrite")
        self._stream_out = value

    @property
    def stream(self):
        return self._stream

    @stream.setter
    def stream(self, value):
        if not(isinstance(value, ZMQSender) and isinstance(value, ZMQReceiver)):
            raise ValueError("stream must be of type ZMQSender AND ZMQReceiver")

        if self.stream_in is not None or self.stream_out is not None:
            raise ValueError("stream_in or stream_out or both are already set")

        self._stream = value
        self.stream_in = value.stream_in
        self.stream_out = value.stream_out
