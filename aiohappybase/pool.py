"""
AIOHappyBase connection pool module.
"""

import logging
import socket
import asyncio as aio
import queue
import threading
from numbers import Real
from typing import Any, Dict, Awaitable

from thriftpy2.thrift import TException

from .connection import Connection

try:
    from asyncio import current_task
except ImportError:  # < 3.7
    current_task = aio.Task.current_task

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


class TaskLocal:
    """
    Wrapper around threading.local() to make it task local as well.

    .. warning::
        We do not use ContextVars because they are not local to the task,
        but rather local to the task stack. This means functions like
        asyncio.gather() and asyncio.shield(), which create new tasks,
        inherit the context and can cause multiple tasks to access the
        same connection.
    """
    def __init__(self):
        # __setattr__ is overridden, avoid infinite recursion
        object.__setattr__(self, '_thread_local', threading.local())

    @property
    def _task_data_by_id(self) -> Dict[int, Dict[str, Any]]:
        """Thread local dictionary mapping task id's to their local data."""
        try:
            return self._thread_local.task_data_by_id
        except AttributeError:
            self._thread_local.task_data_by_id = {}
            return self._thread_local.task_data_by_id

    @property
    def _task_id(self) -> int:
        """Get the current task ID and ensure the task has a data store."""
        try:
            task = current_task()
        except RuntimeError:
            task = None  # Outside event loop
        task_id = id(task)
        if task_id not in self._task_data_by_id:
            self._task_data_by_id[task_id] = {}
            if task is not None:
                task.add_done_callback(self._del_task_callback)
        return task_id

    @property
    def _task_data(self) -> Dict[str, Any]:
        """Get the data store for the current task."""
        return self._task_data_by_id[self._task_id]

    def _del_task_callback(self, task: aio.Task):
        """Clean up data store after the task has finished."""
        del self._task_data_by_id[id(task)]

    def __getattr__(self, item: str) -> Any:
        try:
            return self._task_data[item]
        except KeyError:
            raise AttributeError(item)

    def __setattr__(self, key: str, value: Any) -> None:
        self._task_data[key] = value

    def __delattr__(self, item: str) -> None:
        try:
            del self._task_data[item]
        except KeyError:  # pragma: no cover
            raise AttributeError(item)


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
    Asyncio-safe connection pool.

    .. versionadded:: 0.5

    Connection pools in sync code (like :py:class:`happybase.ConnectionPool`)
    work by creating multiple connections and providing one whenever a thread
    asks. When a thread is done with it, it returns it to the pool to be
    made available to other threads. In async code, instead of threads,
    tasks make the request to the pool for a connection.

    If a task nests calls to :py:meth:`connection`, it will get the
    same connection back, just like in HappyBase.

    The `size` argument specifies how many connections this pool
    manages. Additional keyword arguments are passed unmodified to the
    :py:class:`happybase.Connection` constructor, with the exception of
    the `autoconnect` argument, since maintaining connections is the
    task of the pool.
    """
    QUEUE_TYPE = aio.LifoQueue

    def __init__(self, size: int, **kwargs):
        """
        :param int size: the maximum number of concurrently open connections
        :param kwargs: keyword arguments for Connection
        """
        if not isinstance(size, int):
            raise TypeError("Pool 'size' arg must be an integer")

        if not size > 0:
            raise ValueError("Pool 'size' arg must be greater than zero")

        logger.debug(f"Initializing connection pool with {size} connections")

        self._queue = self.QUEUE_TYPE(maxsize=size)
        self._connections = TaskLocal()

        kwargs['autoconnect'] = False

        for i in range(size):
            self._queue.put_nowait(Connection(**kwargs))

    def close(self):
        """Clean up all pool connections and delete the queue."""
        while True:
            try:
                self._queue.get_nowait().close()
            except (aio.QueueEmpty, queue.Empty):
                break
        del self._queue

    def _queue_get(self, timeout: Real = None) -> Awaitable[Connection]:
        """
        Just get the connection from the queue with a timeout.
        :py:meth:`_acquire_connection` will handle any errors.
        """
        return aio.wait_for(self._queue.get(), timeout)

    async def _acquire_connection(self, timeout: Real = None) -> Connection:
        """Acquire a connection from the pool."""
        try:
            return await self._queue_get(timeout)
        except (aio.TimeoutError, queue.Empty, TimeoutError):
            raise NoConnectionsAvailable("Timeout waiting for a connection")

    async def _return_connection(self, connection: Connection) -> None:
        """Return a connection to the pool."""
        await self._queue.put(connection)

    @asynccontextmanager
    async def connection(self, timeout: Real = None) -> Connection:
        """
        Obtain a connection from the pool.

        This method *must* be used as a context manager, i.e. with
        Python's ``with`` block. Example::

            async with pool.connection() as connection:
                pass  # do something with the connection

        If `timeout` is specified, this is the number of seconds to wait
        for a connection to become available before
        :py:exc:`NoConnectionsAvailable` is raised. If omitted, this
        method waits forever for a connection to become available.

        :param timeout: number of seconds to wait (optional)
        :return: active connection from the pool
        """
        connection = getattr(self._connections, 'current', None)

        return_after_use = False
        if connection is None:
            # This is the outermost connection requests for this task.
            # Obtain a new connection from the pool and keep a reference
            # by the task id so that nested calls get the same connection
            return_after_use = True
            connection = await self._acquire_connection(timeout)
            self._connections.current = connection

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
            connection.close()
            await connection.open()

            # Reraise to caller; see contextlib.contextmanager() docs
            raise

        finally:
            # Remove thread local reference after the outermost 'with'
            # block ends. Afterwards the task no longer owns the connection.
            if return_after_use:
                del self._connections.current
                await self._return_connection(connection)

    # Support async context usage
    async def __aenter__(self) -> 'ConnectionPool':
        return self

    async def __aexit__(self, *_exc) -> None:
        self.close()

    # Support context usage
    def __enter__(self) -> 'ConnectionPool':
        if aio.get_event_loop().is_running():
            raise RuntimeError("Use async with inside a running event loop!")
        return self

    def __exit__(self, *_exc) -> None:
        self.close()

    def __del__(self) -> None:
        if hasattr(self, '_queue'):
            logger.warning(f"{self} was not closed!")
