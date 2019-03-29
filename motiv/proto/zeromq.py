"""0MQ related utilities"""


def create_socket(ctx, socket_type):
    """Creates zmq socket with default attributes
    """
    sock = ctx.socket(socket_type)
    sock.linger = 1000
    sock.rcvtimeo = 1000
    sock.sndhwm = 0
    sock.rcvhwm = 0
    return sock
