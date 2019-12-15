"""
HappyBase connection pool module.
"""

import logging
import queue
from numbers import Real

from .connection import Connection

from ._util import synchronize

logger = logging.getLogger(__name__)


@synchronize
class ConnectionPool:
    """
    Thread-safe connection pool.

    .. versionadded:: 0.5

    Connection pools work by creating multiple connections and providing
    one whenever a thread asks. When a thread is done with it, it returns
    it to the pool to be made available to other threads.

    If a thread nests calls to :py:meth:`connection`, it will get the
    same connection back.

    The `size` argument specifies how many connections this pool
    manages. Additional keyword arguments are passed unmodified to the
    :py:class:`Connection` constructor, with the exception of
    the `autoconnect` argument, since maintaining connections is the
    task of the pool.
    """
    QUEUE_TYPE = queue.LifoQueue

    def _queue_get(self, timeout: Real = None) -> Connection:
        return self._queue.get(block=True, timeout=timeout)
