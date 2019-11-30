"""
HappyBase connection module.
"""

import logging

from thriftpy2.protocol import TBinaryProtocol, TCompactProtocol
from thriftpy2.transport import TBufferedTransport, TFramedTransport
from thriftpy2.transport.socket import TSocket
from thriftpy2.thrift import TClient

from .table import Table
from ._util import synchronize

logger = logging.getLogger(__name__)

STRING_OR_BINARY = (str, bytes)

COMPAT_MODES = ('0.90', '0.92', '0.94', '0.96', '0.98')

DEFAULT_HOST = 'localhost'
DEFAULT_PORT = 9090
DEFAULT_TRANSPORT = 'buffered'
DEFAULT_COMPAT = '0.98'
DEFAULT_PROTOCOL = 'binary'


@synchronize
class Connection:
    # TODO: Auto generate these?
    THRIFT_TRANSPORTS = dict(
        buffered=TBufferedTransport,
        framed=TFramedTransport,
    )
    THRIFT_PROTOCOLS = dict(
        binary=TBinaryProtocol,
        compact=TCompactProtocol,
    )
    THRIFT_SOCKET = TSocket
    THRIFT_CLIENT = TClient

    TABLE_TYPE = Table

    def _autoconnect(self):
        self.open()
