
import pickle

from motiv.serde import Serializable


class DummySerdeMessage(Serializable):
    def __init__(self, field):
        self.field = field

    def serialize(self):
        return pickle.dumps([self])

    @classmethod
    def deserialize(self, payload):
        return pickle.loads(payload)


class DummyNonSerdeMessage(object):

    def __init__(self, field):
        self.field = field
