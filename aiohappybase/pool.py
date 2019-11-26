"""
HappyBase connection pool module.
"""

import logging
import socket
import threading
import queue
from numbers import Real

from thriftpy2.thrift import TException

from .connection import Connection

try:
    from contextlib import asynccontextmanager
except ImportError:  # < 3.7
    from async_generator import asynccontextmanager

logger = logging.getLogger(__name__)

#
# TODO: maybe support multiple Thrift servers. What would a reasonable
# distribution look like? Round-robin? Randomize the list upon
# instantiation and then cycle through it? How to handle (temporary?)
# connection errors?
#


class NoConnectionsAvailable(RuntimeError):
    """
    Exception raised when no connections are available.

    This happens if a timeout was specified when obtaining a connection,
    and no connection became available within the specified timeout.

    .. versionadded:: 0.5
    """
    pass


class ConnectionPool:
    """
    Thread-safe connection pool.

    .. versionadded:: 0.5

    The `size` argument specifies how many connections this pool
    manages. Additional keyword arguments are passed unmodified to the
    :py:class:`happybase.Connection` constructor, with the exception of
    the `autoconnect` argument, since maintaining connections is the
    task of the pool.

    :param int size: the maximum number of concurrently open connections
    :param kwargs:
        keyword arguments passed to :py:class:`happybase.Connection`
    """
    def __init__(self, size: int, **kwargs):
        if not isinstance(size, int):
            raise TypeError("Pool 'size' arg must be an integer")

        if not size > 0:
            raise ValueError("Pool 'size' arg must be greater than zero")

        logger.debug(f"Initializing connection pool with {size} connections")

        self._lock = threading.Lock()
        self._queue = queue.LifoQueue(maxsize=size)
        self._thread_connections = threading.local()

        connection_kwargs = kwargs
        connection_kwargs['autoconnect'] = False

        for i in range(size):
            self._queue.put(Connection(**connection_kwargs))

    def _acquire_connection(self, timeout: Real = None) -> Connection:
        """Acquire a connection from the pool."""
        try:
            return self._queue.get(True, timeout)
        except queue.Empty:
            raise NoConnectionsAvailable(
                "No connection available from pool within specified "
                "timeout")

    def _return_connection(self, connection: Connection) -> None:
        """Return a connection to the pool."""
        self._queue.put(connection)

    @asynccontextmanager
    async def connection(self, timeout: Real = None) -> Connection:
        """
        Obtain a connection from the pool.

        This method *must* be used as a context manager, i.e. with
        Python's ``with`` block. Example::

            with pool.connection() as connection:
                pass  # do something with the connection

        If `timeout` is specified, this is the number of seconds to wait
        for a connection to become available before
        :py:exc:`NoConnectionsAvailable` is raised. If omitted, this
        method waits forever for a connection to become available.

        :param timeout: number of seconds to wait (optional)
        :return: active connection from the pool
        :rtype: :py:class:`happybase.Connection`
        """

        connection = getattr(self._thread_connections, 'current', None)

        return_after_use = False
        if connection is None:
            # This is the outermost connection requests for this thread.
            # Obtain a new connection from the pool and keep a reference
            # in a thread local so that nested connection requests from
            # the same thread can return the same connection instance.
            #
            # Note: this code acquires a lock before assigning to the
            # thread local; see
            # http://emptysquare.net/blog/another-thing-about-pythons-
            # threadlocals/
            return_after_use = True
            connection = self._acquire_connection(timeout)
            with self._lock:
                self._thread_connections.current = connection

        try:
            # Open connection, because connections are opened lazily.
            # This is a no-op for connections that are already open.
            await connection.open()

            # Return value from the context manager's __enter__()
            yield connection

        except (TException, socket.error):
            # Refresh the underlying Thrift client if an exception
            # occurred in the Thrift layer, since we don't know whether
            # the connection is still usable.
            logger.info("Replacing tainted pool connection")
            connection._refresh_thrift_client()
            await connection.open()

            # Reraise to caller; see contextlib.contextmanager() docs
            raise

        finally:
            # Remove thread local reference after the outermost 'with'
            # block ends. Afterwards the thread no longer owns the
            # connection.
            if return_after_use:
                del self._thread_connections.current
                self._return_connection(connection)