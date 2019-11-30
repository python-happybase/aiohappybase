"""
HappyBase connection pool module.
"""

import logging
import queue
import threading
from numbers import Real
from typing import Any

from .connection import Connection

from ._util import synchronize

logger = logging.getLogger(__name__)


class ThreadLocal:
    """
    Wrapper around threading.local() which adds a lock before assigning
    to work around this issue:
    http://emptysquare.net/blog/another-thing-about-pythons-threadlocals/
    """
    def __init__(self):
        self._local = threading.local()
        self._lock = threading.Lock()

    def __getattr__(self, item: str) -> Any:
        return getattr(self._local, item)

    def __setattr__(self, key: str, value: Any) -> None:
        if key in {'_local', '_lock'}:
            return super().__setattr__(key, value)
        with self._lock:
            setattr(self._local, key, value)

    def __delattr__(self, item: str) -> None:
        with self._lock:
            delattr(self._local, item)


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
    CONNECTION_TYPE = Connection
    QUEUE_TYPE = queue.LifoQueue
    LOCAL_TYPE = ThreadLocal

    def _queue_get(self, timeout: Real = None) -> Connection:
        return self._queue.get(block=True, timeout=timeout)
