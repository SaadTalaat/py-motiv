"""multiprocessing based actor and events"""

import abc

from multiprocessing import Process, Event
from ensure import ensure_annotations, ensure

from motiv.sync import SystemEvent
from motiv.exceptions import ActorInitializationError
from motiv.channel import zeromq as zchannels
from motiv.streams import zeromq as zstreams


class ProcessEvent(SystemEvent):
    """Event synchronization primitive.
    Wraps `multiprocessing.Event`
    """
    def __init__(self, *args, **kwargs):
        self.event = Event(*args, **kwargs)

    def set(self):
        """sets the event"""
        return self.event.set()

    def clear(self):
        """clears the event"""
        return self.event.clear()

    def is_set(self):
        """checks if the event is set

        Returns:
            bool: True if set, False if cleared
        """
        return self.event.is_set()


class ExecutionContextBase(Process, abc.ABC):

    """Mixin containing default behavior of an actor loop.

    Args:
        name(str): actor name

    Attributes:
        name(str): actor name
    """
    def __init__(self, name: str):
        self.name = name
        self._halt = ProcessEvent()
        super().__init__(name=name)

    @abc.abstractmethod
    def pre_start(self):
        """executes before starting an actor

        Note:
            this method runs in the actor child-process
            meaning streams, channels, sockets initialized
            in this function is owned by the actor process
        """

    @abc.abstractmethod
    def post_stop(self):
        """executes after the actor has stopped
        close streams, sockets, descriptors in this function
        """

    @abc.abstractmethod
    def tick(self):
        """actor tick handler

        an actor tick is occurs on each event loop cycle.

        Note:
            this function should not be blocking. minimal
            behavior is recommended, if it turns out to
            be a complex function then consider breaking
            the actor into two actors.
        """

    def stop(self):
        """halts the actor"""
        return self._halt.set()

    @property
    def runnable(self):
        """bool: True if actor hasn't been signaled to stop,False otherwise"""
        return not self._halt.is_set()


class ExecutionContext(ExecutionContextBase):
    """Actual actor definition.

    Args:
        name(str): actor name.

    Attributes:
        name: actor name.
        poller: stream poller.
    """

    def __init__(self, name):
        self.name = name
        super().__init__(self.name)
        self._stream_in = None
        self._stream_out = None
        self._stream = None
        self.poller = zchannels.Poller()

        # Properties
        self._poll_timeout = 5

    def receive(self):
        """polls input stream for data"""
        if not self.stream_in:
            raise ActorInitializationError("No in-stream set")
        return self.stream_in.poll(self.poller, self._halt, self.poll_timeout)

    def send(self, payload):
        """sends data over the output stream"""
        if not self.stream_out:
            raise ActorInitializationError("No out-stream set")
        return self.stream_out.send(payload)

    def publish(self, topic, payload):
        """publishes data over the output stream"""
        ensure(self.stream_out).is_a(zstreams.Emitter)
        return self.stream_out.publish(topic, payload)

    @abc.abstractmethod
    def run(self):
        """process body"""

    @ensure_annotations
    def set_stream(self, stream):
        """assigns a given stream to out-stream, in-stream."""
        if isinstance(stream, zstreams.Sender) and \
                isinstance(stream, zstreams.Receiver):
            self.stream = stream
        elif isinstance(stream, zstreams.Sender):
            self.stream_out = stream
        elif isinstance(stream, zstreams.Receiver):
            self.stream_in = stream
        else:
            raise ValueError("Incompatible stream type")

    @property
    def stream_in(self):
        """actor input stream"""
        return self._stream_in

    @stream_in.setter
    @ensure_annotations
    def stream_in(self, value: zstreams.Receiver):
        if self.stream is not None:
            raise ValueError("stream_in is set from stream,"
                             " reset stream to overwrite")
        self._stream_in = value

    @property
    def stream_out(self):
        """actor output stream"""
        return self._stream_out

    @stream_out.setter
    def stream_out(self, value: zstreams.Sender):
        if self.stream is not None:
            raise ValueError("stream_out is set from stream,"
                             " reset stream to overwrite")
        self._stream_out = value

    @property
    def stream(self):
        """actor duplex stream, if exists"""
        return self._stream

    @stream.setter
    def stream(self, value):
        if not(isinstance(value, zstreams.Sender) and
               isinstance(value, zstreams.Receiver)):
            raise ValueError("stream must be of type Sender AND Receiver")

        if self.stream_in is not None or self.stream_out is not None:
            raise ValueError("stream_in or stream_out or both are already set")

        self._stream = value
        self.stream_in = value.stream_in
        self.stream_out = value.stream_out

    @property
    def poll_timeout(self):
        """int: actor's poller poll timeout"""
        return self._poll_timeout

    @poll_timeout.setter
    def poll_timeout(self, value):
        ensure(value).is_an(int)
        self._poll_timeout = value


class Ticker(ExecutionContext):

    def run(self):
        """actor process body"""
        self.pre_start()
        while self.runnable:
            self.tick()

        self.post_stop()


class Proxy(ExecutionContext):
    """Stream proxy actor"""

    def proxy(self):
        """The actor becomes a proxy"""
        if not(self.stream or (self.stream_in and self.stream_out)):
            raise ActorInitializationError("No streams set")

        if not self.stream:
            self.stream = zstreams.CompoundStream(
                self.stream_in, self.stream_out)

        return self.stream.run()

    def run(self):
        """actor process body"""
        self.pre_start()
        self.proxy()
        self.post_stop()
