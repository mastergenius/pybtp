"""
Python api for btp (https://github.com/mambaru/btp-daemon)

Example:
import pybtp

conn = pybtp.Connection('example.com', 22400)
conn.connect()
req = pybtp.Request(conn, 'btp.test')
req.append('btp.test.service', 'srv1', 'test', 4321)
cntr = pybtp.Counter(req, 'btp.counter.test', 'srv1', 'test.counter')
cntr.stop()
req.close()
conn.disconnect()
"""

import json
import re
import socket
import time

try:
    import resource
except ImportError:
    resource = None


def micro_delta(b, a):
    return int(1000000 * (b - a))


class Connection(object):

    def __init__(self, host, port):
        self._address = (host, port)
        self.failed = False

    def connect(self):
        self._socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._socket.connect(self._address)


    def _send(self, data):
        self._socket.sendall(json.dumps(data, separators=(',',':')) + "\r\n")

    def disconnect(self):
        self._socket.close()

    def notify(self, method, params):
        self._send(dict(jsonrpc='2.0', method=method, params=params))


class Request(object):

    farm_re = re.compile(r'\d')


    def __init__(self, connection, script):
        """
        @type connection Connection
        """
        self._connection = connection
        self.script = script
        self.ts = time.time()
        self.items = {}
        self.items_count = 0
        self.server = socket.gethostname()
        if resource:
            self._start_rusage = resource.getrusage(resource.RUSAGE_SELF)

    def _send(self):
        data = dict(
            srv=self.server,
            script=self.script,
            time=self.ts,
            items=self.items
        )
        self._connection.notify('put', data)
        self.items = {}
        self.items_count = 0

    def _append_script_timings(self):
        script_service = "SCRIPT_%s" % self.server
        farm = re.sub(self.farm_re, '', self.server)
        farm_stats = self.items.setdefault(script_service, {}).setdefault(farm, {})
        farm_stats['all'] = [micro_delta(time.time(), self.ts)]
        if resource:
            end_rusage = resource.getrusage(resource.RUSAGE_SELF)
            farm_stats['user'] = [
                micro_delta(end_rusage.ru_utime, self._start_rusage.ru_utime)]
            farm_stats['system'] = [
                micro_delta(end_rusage.ru_stime, self._start_rusage.ru_stime)]

    def append(self, service, server, operation, timing):
        server_stats = self.items.setdefault(service, {}).setdefault(server, {})
        if operation not in server_stats:
            server_stats[operation] = []
            self.items_count += 1
        server_stats[operation].append(timing)

        if self.items_count > 30 or time.time() - self.ts > 1:
            self._send()

    def close(self):
        self._append_script_timings()
        self._send()


class Counter(object):

    def __init__(self, request, service, server, operation):
        """
        @type request Request
        """
        self.request = request
        self.service = service
        self.server = server
        self.operation = operation
        self.ts = time.time()

    def stop(self):
        self.request.append(
            self.service, self.server, self.operation,
            micro_delta(time.time(), self.ts))
