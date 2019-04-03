import unittest
import time

from concurrent.futures import ThreadPoolExecutor

from motiv import exceptions as excs
from motiv.streams import (EmitterType, SubscriberType,
        VentilatorType, WorkerType, SinkType)
from motiv.streams import zeromq as zstreams

from motiv.test.fixtures import msg


class TestEmitter(unittest.TestCase):

    def setUp(self):
        self.emitter = zstreams.Emitter("/tmp/emitter_test", "ipc")
        self.emitter.connect()

    def tearDown(self):
        self.emitter.close()

    def test_inheritance(self):

        self.assertTrue(issubclass(zstreams.Emitter, EmitterType))
        self.assertTrue(issubclass(zstreams.Emitter, zstreams.Sender))

        self.assertFalse(issubclass(zstreams.Emitter, zstreams.Receiver))

    def test_send_before_connecting(self):
        emitter = zstreams.Emitter("/tmp/emitter_test_doesnt_exist", "ipc")
        with self.assertRaises(excs.NotConnected):
            emitter.send("foo")

    def test_send_wrong_type(self):
        with self.assertRaises(TypeError):
            self.emitter.send("some_string")

    def test_send_bytes(self):
        self.emitter.send(b"byte_array")

    def test_send_non_serializable_object(self):
        o = object()
        with self.assertRaises(TypeError):
            self.emitter.send(o)

    def test_send_serializable(self):
        m = msg.DummySerdeMessage("field")
        self.emitter.send(m)

    def test_publish_before_connecting(self):
        emitter = zstreams.Emitter("/tmp/emitter_test_doesnt_exist", "ipc")
        with self.assertRaises(excs.NotConnected):
            emitter.publish(1, b"foo")

    def test_publish_wrong_type(self):
        with self.assertRaises(TypeError):
            self.emitter.publish("topic", b"message")

        with self.assertRaises(TypeError):
            self.emitter.publish(1, "message")

    def test_publish_right_types(self):
        self.emitter.publish(1, b"message")

    def test_publish_non_serializable_object(self):
        o = object()
        with self.assertRaises(TypeError):
            self.emitter.publish(1, o)

    def test_publish_serializable(self):
        m = msg.DummySerdeMessage("field")
        self.emitter.publish(1, m)


class TestSubscriber(unittest.TestCase):

    def setUp(self):
        self.emitter = zstreams.Emitter("/tmp/subtest", "ipc")
        self.subscriber = zstreams.Subscriber("/tmp/subtest", "ipc")
        self.emitter.connect()
        self.subscriber.connect()

    def tearDown(self):
        self.subscriber.close()
        self.emitter.close()

    def test_inheritance(self):

        self.assertTrue(issubclass(zstreams.Subscriber, SubscriberType))
        self.assertTrue(issubclass(zstreams.Subscriber, zstreams.Receiver))

        self.assertFalse(issubclass(zstreams.Subscriber, zstreams.Sender))

    def test_receive_before_connecting(self):
        sub = zstreams.Subscriber("/tmp/subscriber_test_doesnt_exist", "ipc")
        with self.assertRaises(excs.NotConnected):
            sub.receive()

    def test_subscribed_behavior(self):
        pool = ThreadPoolExecutor(max_workers=1)
        self.subscriber.subscribe(1)

        result = pool.submit(self.subscriber.receive)
        # Yield GIL
        time.sleep(0.001)
        self.emitter.publish(1, b"test")
        channel, payload = result.result()

        self.assertEqual(payload, b"test")
        self.assertEqual(channel, bytes([1]))

    def test_receive_serialized_object(self):
        pool = ThreadPoolExecutor(max_workers=1)
        m = msg.DummySerdeMessage("field")
        self.subscriber.subscribe(1)

        result = pool.submit(self.subscriber.receive)
        # Yield GIL
        time.sleep(0.001)
        self.emitter.publish(1, m)
        channel, payload = result.result()
        objs = msg.DummySerdeMessage.deserialize(payload)

        self.assertEqual(len(objs), 1)
        o = objs[0]
        self.assertEqual(o.field, m.field)


class TestVentilator(unittest.TestCase):

    def setUp(self):
        self.vent = zstreams.Ventilator("/tmp/vent_test", "ipc")
        self.vent.connect()
        self.worker = zstreams.Worker("/tmp/vent_test", "ipc")
        self.worker.connect()

    def tearDown(self):
        self.worker.close()
        self.vent.close()

    def test_inheritance(self):

        self.assertTrue(issubclass(zstreams.Ventilator, VentilatorType))
        self.assertTrue(issubclass(zstreams.Ventilator, zstreams.Sender))

        self.assertFalse(issubclass(zstreams.Ventilator, zstreams.Receiver))

    def test_send_before_connecting(self):
        vent = zstreams.Ventilator("/tmp/vent_test_doesnt_exist", "ipc")
        with self.assertRaises(excs.NotConnected):
            vent.send("foo")

    def test_send_wrong_type(self):
        with self.assertRaises(TypeError):
            self.vent.send("some_string")

    def test_send_bytes(self):
        self.vent.send(b"byte_array")

    def test_send_non_serializable_object(self):
        o = object()
        with self.assertRaises(TypeError):
            self.vent.send(o)

    def test_send_serializable(self):
        m = msg.DummySerdeMessage("field")
        self.vent.send(m)

class TestWorker(unittest.TestCase):

    def setUp(self):
        self.vent = zstreams.Ventilator("/tmp/vent_test", "ipc")
        self.vent.connect()
        self.worker = zstreams.Worker("/tmp/vent_test", "ipc")
        self.worker.connect()

    def tearDown(self):
        self.worker.close()
        self.vent.close()

    def test_inheritance(self):
        self.assertTrue(issubclass(zstreams.Worker, WorkerType))
        self.assertTrue(issubclass(zstreams.Worker, zstreams.Receiver))

        self.assertFalse(issubclass(zstreams.Worker, zstreams.Sender))

    def test_receive_before_connecting(self):
        worker = zstreams.Worker("/tmp/worker_test_doesnt_exist", "ipc")
        with self.assertRaises(excs.NotConnected):
            worker.receive()

    def test_worker_behavior(self):
        pool = ThreadPoolExecutor(max_workers=1)

        result = pool.submit(self.worker.receive)
        # Yield GIL
        time.sleep(0.001)
        self.vent.send(b"test")
        payload = result.result()
        payload = payload[0]
        self.assertEqual(payload, b"test")

    def test_receive_serialized_object(self):
        pool = ThreadPoolExecutor(max_workers=1)
        m = msg.DummySerdeMessage("field")

        result = pool.submit(self.worker.receive)
        # Yield GIL
        time.sleep(0.001)
        self.vent.send(m)
        payload = result.result()
        payload = payload[0]
        objs = msg.DummySerdeMessage.deserialize(payload)

        self.assertEqual(len(objs), 1)
        o = objs[0]
        self.assertEqual(o.field, m.field)

class TestSink(unittest.TestCase):

    def setUp(self):
        self.vent = zstreams.Ventilator("/tmp/vent_test", "ipc")
        self.sink = zstreams.Sink("/tmp/sink_test", "ipc")
        worker = zstreams.Worker("/tmp/vent_test", "ipc")
        worker_vent = zstreams.Ventilator("/tmp/sink_test", "ipc")
        self.worker = zstreams.CompoundStream(worker, worker_vent)

        def run_proxy():
            self.worker.stream_in.connect()
            # TODO: Make special stream type for pusher streams
            self.worker.stream_out.channel_out.connect()
            self.worker.run()

        self.vent.connect()
        self.pool = ThreadPoolExecutor(max_workers=1)
        self.sink.connect()
        self.pool.submit(run_proxy)

    def tearDown(self):
        self.worker.close()
        self.pool.shutdown()
        self.vent.close()
        self.sink.close()

    def test_inheritance(self):
        self.assertTrue(issubclass(zstreams.Sink, SinkType))
        self.assertTrue(issubclass(zstreams.Sink, zstreams.Receiver))

        self.assertFalse(issubclass(zstreams.Sink, zstreams.Sender))

    def test_receive_before_connecting(self):
        sink = zstreams.Sink("/tmp/sink_test_doesnt_exist", "ipc")
        with self.assertRaises(excs.NotConnected):
            sink.receive()

    def test_sink_behavior(self):
        pool = ThreadPoolExecutor(max_workers=1)

        result = pool.submit(self.sink.receive)
        # Yield GIL
        time.sleep(0.001)
        self.vent.send(b"test")
        payload = result.result()
        payload = payload[0]
        self.assertEqual(payload, b"test")

    def test_receive_serialized_object(self):
        pool = ThreadPoolExecutor(max_workers=1)
        m = msg.DummySerdeMessage("field")

        result = pool.submit(self.sink.receive)
        # Yield GIL
        time.sleep(0.001)
        self.vent.send(m)
        payload = result.result()
        payload = payload[0]
        objs = msg.DummySerdeMessage.deserialize(payload)

        self.assertEqual(len(objs), 1)
        o = objs[0]
        self.assertEqual(o.field, m.field)


class TestCompoundStream(unittest.TestCase):

    def setUp(self):
        # Ventilating subscriber
        self.emitter = zstreams.Emitter("/tmp/vent_sub_test", "ipc")
        subscriber = zstreams.Subscriber("/tmp/vent_sub_test", "ipc")
        ventilator = zstreams.Ventilator("/tmp/vent_sub_test_out", "ipc")
        self.vent_sub = zstreams.CompoundStream(subscriber, ventilator)
        self.worker = zstreams.Worker("/tmp/vent_sub_test_out", "ipc")

        def run_proxy():
            self.vent_sub.stream_in.subscribe(1)
            self.vent_sub.stream_in.connect()
            self.vent_sub.stream_out.connect()
            self.vent_sub.run()

        self.worker.connect()
        self.emitter.connect()
        self.pool = ThreadPoolExecutor(max_workers=1)
        self.pool.submit(run_proxy)

    def tearDown(self):
        self.emitter.close()
        self.vent_sub.close()
        self.pool.shutdown()
        self.worker.close()

    def test_channel_is_duplex(self):
        self.assertEqual(self.vent_sub.channel_out, self.vent_sub.channel_in)

    def test_stream_behavior(self):
        self.emitter.publish(1, b"foo")
        # Yield GIL to proxy
        time.sleep(0.1)
        channel, payload = self.worker.receive()
        self.assertEqual(payload, b"foo")
        self.assertEqual(channel, bytes([1]))
