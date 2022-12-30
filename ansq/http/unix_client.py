import http.client
import socket


class UnixHTTPConnection(http.client.HTTPConnection):
    """HTTPConnection that uses a Unix Domain Socket"""

    def __init__(self, path, timeout=socket._GLOBAL_DEFAULT_TIMEOUT, blocksize=8192):
        self.path = path
        self.timeout = timeout
        self.blocksize = blocksize
        self.sock = None
        self._buffer = []
        self._method = None
        self._tunnel_host = None
        self._tunnel_port = None
        self._tunnel_headers = {}

        self._HTTPConnection__response = None
        self._HTTPConnection__state = http.client._CS_IDLE

        self.host = "localhost"
        self.port = 0

    def connect(self):
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.sock.settimeout(self.timeout)
        self.sock.connect(self.path)

    def __repr__(self):
        return '<UnixHTTPConnection: %s>' % self.path

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def make_request(self, method, url, **kwargs):
        self.request(method, url, **kwargs)
        return self.getresponse()
