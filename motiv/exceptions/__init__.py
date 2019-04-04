"""motiv exceptions

"""


class ActorInitializationError(Exception):
    """Exception to denote failure to initialize an actor"""


class AlreadyConnected(Exception):
    """Exception that occurs connecting more than once"""


class NotConnected(Exception):
    """Exception to denote that a channel isn't connected"""


class ScalingError(Exception):
    """Exception to indicate an error encountered whiel scaling group"""


class DispatchError(Exception):
    """Exception to denote issue to dispatch, stop an actor"""
