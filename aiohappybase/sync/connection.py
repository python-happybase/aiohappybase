"""
HappyBase connection module.
"""

import logging

from thriftpy2.transport import (
    TBufferedTransportFactory,
    TFramedTransportFactory,
)
from thriftpy2.protocol import (
    TBinaryProtocolFactory,
    TCompactProtocolFactory,
)
from thriftpy2.transport.socket import TSocket
from thriftpy2.thrift import TClient

from ._util import synchronize

logger = logging.getLogger(__name__)


@synchronize
class Connection:
    # TODO: Auto generate these?
    THRIFT_TRANSPORTS = dict(
        buffered=TBufferedTransportFactory(),
        framed=TFramedTransportFactory(),
    )
    THRIFT_PROTOCOLS = dict(
        binary=TBinaryProtocolFactory(decode_response=False),
        compact=TCompactProtocolFactory(decode_response=False),
    )
    THRIFT_SOCKET = TSocket
    THRIFT_CLIENT = TClient

    def _autoconnect(self):
        self.open()


# Set the default value for autoconnect to True for backwards compatibility
_d = Connection.__init__.__defaults__
Connection.__init__.__defaults__ = *_d[:3], True, *_d[4:]
