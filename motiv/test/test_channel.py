
import unittest
import zmq

from concurrent.futures import ThreadPoolExecutor

from motiv import exceptions as excs
from motiv.channel import ChannelOutType, ChannelInType, ChannelType
from motiv.channel import zeromq as zchan
from motiv.actor.process import ProcessEvent

from motiv.test.fixtures import msg


class TestChannelOut(unittest.TestCase):

    def setUp(self):
        self.cout = zchan.ChannelOut(zmq.PUB,
                "ipc", "/tmp/test_channel_out")
        self.cout.bind()

    def tearDown(self):
        self.cout.close()

    def test_inheritance(self):
        self.assertTrue(issubclass(zchan.ChannelOut, ChannelOutType))
        self.assertTrue(issubclass(zchan.Channel, ChannelType))

    def test_arg_wrong_types(self):

        with self.assertRaises(TypeError):
            self.cout.send("test_passing_a_string")
        with self.assertRaises(TypeError):
            self.cout.send(1)
        with self.assertRaises(TypeError):
            self.cout.send(1.1)
        with self.assertRaises(TypeError):
            self.cout.send(dict(foo="bar"))

    def test_arg_byte_type(self):
        self.cout.send(b"foo")
        self.cout.send((b"foo", b"bar"))
        self.cout.send([b"foo", b"bar"])

    def test_arg_serializable_types(self):

        m = msg.DummySerdeMessage("somefield")
        self.cout.send(m)

    def test_arg_non_serializable(self):
        m = msg.DummyNonSerdeMessage("samefield")
        with self.assertRaises(TypeError):
            self.cout.send(m)

    def test_bind_twice(self):
        chan = zchan.ChannelOut(zmq.PUB,
                "ipc", "/tmp/test_bind_twice")
        chan.bind()
        with self.assertRaises(excs.AlreadyConnected):
            chan.bind()
        chan.close()

    def test_connect_twice(self):
        chan = zchan.ChannelOut(zmq.PUB,
                "ipc", "/tmp/test_connect_twice")
        chan.connect()
        with self.assertRaises(excs.AlreadyConnected):
            chan.connect()
        chan.close()

    def test_send_without_connecting(self):
        chan = zchan.ChannelOut(zmq.PUB,
                "ipc", "/tmp/test_i_dont_exist")
        with self.assertRaises(excs.NotConnected):
            chan.send(b"foo")
        chan.close()


class TestChannelIn(unittest.TestCase):

    def setUp(self):
        import zmq
        self.cout = zchan.ChannelOut(zmq.PUSH,
                "ipc", "/tmp/test_channel_in")
        self.cin = zchan.ChannelIn(zmq.PULL,
                "ipc", "/tmp/test_channel_in")

        self.cout.bind()
        self.cin.connect()

    def test_inheritance(self):
        self.assertTrue(issubclass(zchan.ChannelIn, ChannelInType))

    def tearDown(self):
        self.cout.close()
        self.cin.close()

    def test_receive_return_type(self):
        self.cout.send(b"foo")
        payload = self.cin.receive()
        payload = payload[0]
        self.assertIsInstance(payload, bytes)
        self.assertEqual(payload, b"foo")

    def test_receive_serializable(self):
        m = msg.DummySerdeMessage("test_field")
        self.cout.send(m)

        msgs = self.cin.receive()
        payload = msgs[0]
        m2 = msg.DummySerdeMessage.deserialize(payload)
        m2 = m2[0]

        self.assertIsInstance(m2, msg.DummySerdeMessage)
        self.assertEqual(m2.field, m.field)

    def test_poll_not_connected(self):
        chan = zchan.ChannelIn(zmq.PULL,
                "ipc", "/tmp/test_bind_twice_in")
        with self.assertRaises(excs.NotConnected):
            pollr = zchan.Poller()
            exit_condition = ProcessEvent()
            chan.poll(pollr, exit_condition)

    def test_auto_register_socket(self):
        chan = zchan.ChannelIn(zmq.PULL,
                "ipc", "/tmp/test_bind_twice_in")
        chan.bind()

        pollr = zchan.Poller()
        exit_condition = ProcessEvent()
        # set condition to immediately exit event loop
        exit_condition.set()

        self.assertEqual(len(pollr.sockets), 0)
        chan.poll(pollr, exit_condition)
        self.assertEqual(len(pollr.sockets), 1)


    def test_polling(self):
        chan = zchan.ChannelIn(zmq.PULL,
                "ipc", "/tmp/test_bind_twice_in")
        chan.bind()
        pollr = zchan.Poller()
        exit_condition = ProcessEvent()
        pool = ThreadPoolExecutor(max_workers=1)

        def send_something():
            chan = zchan.ChannelOut(zmq.PUSH,
                    "ipc", "/tmp/test_bind_twice_in")
            chan.connect()
            chan.send(b"something")

        # set condition to immediately exit event loop
        pool.submit(send_something)
        self.assertEqual(len(pollr.sockets), 0)
        chan.poll(pollr, exit_condition)
        self.assertEqual(len(pollr.sockets), 1)
        pool.shutdown()

    def test_bind_twice(self):
        chan = zchan.ChannelIn(zmq.PULL,
                "ipc", "/tmp/test_bind_twice_in")
        chan.bind()
        with self.assertRaises(excs.AlreadyConnected):
            chan.bind()
        chan.close()

    def test_connect_twice(self):
        chan = zchan.ChannelIn(zmq.PULL,
                "ipc", "/tmp/test_connect_twice_in")
        chan.connect()
        with self.assertRaises(excs.AlreadyConnected):
            chan.connect()
        chan.close()

    def test_send_without_connecting(self):
        chan = zchan.ChannelIn(zmq.PULL,
                "ipc", "/tmp/test_i_dont_exist")
        with self.assertRaises(excs.NotConnected):
            chan.receive()
        chan.close()


class TestChannel(unittest.TestCase):

    def setUp(self):
        self.cin = zchan.ChannelIn(zmq.PULL,
                "ipc", "/tmp/test_channel_socket_in")
        self.cout = zchan.ChannelOut(zmq.PUB,
                "ipc", "/tmp/test_channel_socket_out")
        self.cin.bind()
        self.cout.bind()
        self.chan = zchan.Channel(self.cin, self.cout)

    def tearDown(self):
        self.chan.close()

    def test_inheritance(self):
        self.assertTrue(issubclass(zchan.Channel, ChannelInType))
        self.assertTrue(issubclass(zchan.Channel, ChannelOutType))

    def test_send_wrong_type(self):
        with self.assertRaises(TypeError):
            self.chan.send("foo")
#
    def test_send_bytes(self):
        self.chan.send(b"foo")

    def test_receive(self):
        chan_out = zchan.ChannelOut(zmq.PUSH,
                "ipc", "/tmp/test_channel_socket_in")
        chan_out.connect()
        chan_out.send(b"foo")
        m = self.chan.receive()
        self.assertEqual(m, [b"foo"])
        chan_out.close()

    def test_poll(self):
        pollr = zchan.Poller()
        exit_condition = ProcessEvent()
        pool = ThreadPoolExecutor(max_workers=1)

        def send_something():
            chan = zchan.ChannelOut(zmq.PUSH,
                    "ipc", "/tmp/test_channel_socket_in")
            chan.connect()
            chan.send(b"something")

        # set condition to immediately exit event loop
        pool.submit(send_something)
        self.assertEqual(len(pollr.sockets), 0)
        self.chan.poll(pollr, exit_condition)
        self.assertEqual(len(pollr.sockets), 1)
        pool.shutdown()

    def test_proxy_not_connected(self):
        cin = zchan.ChannelIn(zmq.PULL,
                "ipc", "/tmp/test_channel_socket_in")
        cout = zchan.ChannelOut(zmq.PUB,
                "ipc", "/tmp/test_channel_socket_out")
        chan = zchan.Channel(cin, cout)

        with self.assertRaises(excs.NotConnected):
            chan.proxy()

    def test_proxy(self):
        pool = ThreadPoolExecutor(max_workers=1)
        # Thread reaped on test-case teardown
        pool.submit(self.chan.proxy)

class TestPoller(unittest.TestCase):
    def setUp(self):
        self.cin = zchan.ChannelIn(zmq.PULL,
                "ipc", "/tmp/test_channel_socket_in")
        self.cout = zchan.ChannelOut(zmq.PUB,
                "ipc", "/tmp/test_channel_socket_out")
        self.cin.bind()
        self.cout.bind()

    def tearDown(self):
        self.cin.close()
        self.cout.close()

    def test_register_channel_in(self):
        pollr = zchan.Poller()
        pollr.register_channel(self.cin)
        self.assertEqual(len(pollr.sockets), 1)

    def test_register_channel_out_twice(self):
        pollr = zchan.Poller()
        pollr.register_channel(self.cin)
        self.assertEqual(len(pollr.sockets), 1)
        pollr.register_channel(self.cin)
        self.assertEqual(len(pollr.sockets), 1)

    def test_register_channel_out(self):
        pollr = zchan.Poller()
        pollr.register_channel(self.cout)
        self.assertEqual(len(pollr.sockets), 1)

    def test_register_channel_out_twice(self):
        pollr = zchan.Poller()
        pollr.register_channel(self.cout)
        self.assertEqual(len(pollr.sockets), 1)
        pollr.register_channel(self.cout)
        self.assertEqual(len(pollr.sockets), 1)

    def test_unregister_channel_in(self):
        pollr = zchan.Poller()
        pollr.register_channel(self.cin)
        self.assertEqual(len(pollr.sockets), 1)
        pollr.unregister_channel(self.cin)
        self.assertEqual(len(pollr.sockets), 0)

    def test_unregister_channel_in_twice(self):
        pollr = zchan.Poller()
        pollr.register_channel(self.cin)
        self.assertEqual(len(pollr.sockets), 1)
        pollr.unregister_channel(self.cin)
        self.assertEqual(len(pollr.sockets), 0)
        with self.assertRaises(KeyError):
            pollr.unregister_channel(self.cin)

    def test_unregister_channel_out(self):
        pollr = zchan.Poller()
        pollr.register_channel(self.cout)
        self.assertEqual(len(pollr.sockets), 1)
        pollr.unregister_channel(self.cout)
        self.assertEqual(len(pollr.sockets), 0)

    def test_unregister_channel_in_twice(self):
        pollr = zchan.Poller()
        pollr.register_channel(self.cout)
        self.assertEqual(len(pollr.sockets), 1)
        pollr.unregister_channel(self.cout)
        self.assertEqual(len(pollr.sockets), 0)
        with self.assertRaises(KeyError):
            pollr.unregister_channel(self.cout)


    def test_wrong_channel_type(self):
        pollr = zchan.Poller()
        with self.assertRaises(Exception):
            pollr.register_channel("Foo")

        with self.assertRaises(Exception):
            pollr.unregister_channel("Foo")
