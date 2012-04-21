"""
Example script
"""

import pybtp

conn = pybtp.Connection('example.com', 22400)
conn.connect()
req = pybtp.Request(conn, 'btp.test')
req.append('btp.test.service', 'srv1', 'test', 4321)
cntr = pybtp.Counter(req, 'btp.counter.test', 'srv1', 'test.counter')
cntr.stop()
req.close()
conn.disconnect()
