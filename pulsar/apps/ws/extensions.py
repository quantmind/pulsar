import zlib

from pulsar.utils import websocket

############################################################################
#  x-webkit-deflate-frame     Extension
#
# http://code.google.com/p/pywebsocket/


class deflate_frame(websocket.Extension):

    def __init__(self, window_bits=None):
        self.window_bits = window_bits or zlib.MAX_WBITS

    def receive(self, frame, application_data):
        if frame.rsv1 == 1:
            application_data += b'\x00\x00\xff\xff'
            return zlib.decompress(application_data)
        else:
            return application_data

    def send(self, frame, application_data):
        application_data = self._compress.compress(application_data)
        return application_data


# websocket.WS_EXTENSIONS['x-webkit-deflate-frame'] = deflate_frame
