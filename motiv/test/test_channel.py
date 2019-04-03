
import unittest
import zmq

from motiv import exceptions as excs
from motiv.channel import ChannelOutType, ChannelInType, ChannelType
from motiv.channel import zeromq as zchan

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


class TestChannel(unittest.TestCase):

    def test_inheritance(self):
        self.assertTrue(issubclass(zchan.Channel, ChannelInType))
        self.assertTrue(issubclass(zchan.Channel, ChannelOutType))

    def test_bind_twice(self):
        chan = zchan.ChannelOut(zmq.PUB,
                "ipc", "/tmp/test_connect_twice")
        chan.bind()
        with self.assertRaises(excs.AlreadyConnected):
            chan.bind()

    def test_send_without_connecting(self):
        chan = zchan.ChannelOut(zmq.PUB,
                "ipc", "/tmp/test_i_dont_exist")
        with self.assertRaises(excs.NotConnected):
            chan.send(b"foo")


