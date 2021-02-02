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
from thriftpy2.rpc import make_client

from ._util import synchronize

logger = logging.getLogger(__name__)

try:
    from thriftpy2_httpx_client import make_sync_client as make_http_client
except ImportError:  # pragma: no cover
    def make_http_client(*_, **__):
        raise RuntimeError("thriftpy2_httpx_client is required to"
                           " use the HTTP client protocol.")


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
    THRIFT_CLIENTS = dict(
        socket=make_client,
        http=make_http_client,
    )

    def _autoconnect(self):
        self.open()


# Set the default value for autoconnect to True for backwards compatibility
_d = Connection.__init__.__defaults__
Connection.__init__.__defaults__ = *_d[:3], True, *_d[4:]
