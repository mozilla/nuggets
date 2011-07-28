import socket
import time


class GraphiteError(Exception):
    pass


class GraphiteClient(object):

    def __init__(self, host='localhost', port=2003, prefix='', timeout=1):
        self.host = host
        self.port = port
        self.prefix = prefix
        self.timeout = timeout
        self.sock = None

    def socket_error(self, e):
        if len(e.args) == 1:
            msg = ('Error connecting to %s:%s. %s.' %
                   (self.host, self.port, e.args[0]))
        else:
            msg = ('Error %s connecting %s:%s. %s.' %
                   (e.args[0], self.host, self.port, e.args[1]))
        return GraphiteError(msg)

    def connect(self):
        if self.sock:
            return
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(self.timeout)
            sock.connect((self.host, self.port))
        except socket.error, e:
            raise self.socket_error(e)
        self.sock = sock

    def disconnect(self):
        if not self.sock:
            return
        try:
            self.sock.close()
        except socket.error:
            pass
        self.sock = None

    def send(self, key, value, timestamp=None):
        if timestamp:
            self.sendall(*[(key, value, timestamp)])
        else:
            self.sendall(*[(key, value)])

    def sendall(self, *items):
        ts = time.time()
        prefix = ('%s.' % self.prefix) if self.prefix else ''
        msg = []
        for item in items:
            if len(item) == 2:
                (key, value), timestamp = item, ts
            else:
                key, value, timestamp = item
            msg.append('%s%s %s %s\n' % (prefix, key, value, timestamp))

        self.connect()
        try:
            self.sock.sendall(''.join(msg))
        except socket.error, e:
            self.disconnect()
            raise self.socket_error(e)


try:
    from django.conf import settings
    host = getattr(settings, 'GRAPHITE_HOST', 'localhost')
    port = getattr(settings, 'GRAPHITE_PORT', 2003)
    prefix = getattr(settings, 'GRAPHITE_PREFIX', '')
    timeout = getattr(settings, 'GRAPHITE_TIMEOUT', 1)
    graphite = GraphiteClient(host, port, prefix, timeout)
except ImportError:
    pass
