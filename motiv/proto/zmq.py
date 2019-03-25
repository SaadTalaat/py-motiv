
import zmq

def create_socket(ctx, socket_type):

    sock = ctx.socket(socket_type)
    sock.linger = 1000
    sock.rcvtimeo = 1000
    sock.sndhwm = 0
    sock.rcvhwm = 0
    return sock
