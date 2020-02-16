"""
AIOHappyBase connection module.
"""

import os
import logging
from typing import AnyStr, List, Dict, Any

from pkg_resources import parse_version

import thriftpy2
from thriftpy2.contrib.aio.transport import (
    TAsyncBufferedTransportFactory,
    TAsyncFramedTransportFactory,
)
from thriftpy2.contrib.aio.protocol import (
    TAsyncBinaryProtocolFactory,
    TAsyncCompactProtocolFactory,
)
from thriftpy2.contrib.aio.rpc import make_client

from Hbase_thrift import Hbase, ColumnDescriptor

from .table import Table
from ._util import (
    ensure_bytes,
    snake_to_camel_case,
    check_invalid_items,
    run_coro,
)

logger = logging.getLogger(__name__)

COMPAT_MODES = ('0.90', '0.92', '0.94', '0.96', '0.98')

DEFAULT_HOST = os.environ.get('AIOHAPPYBASE_HOST', 'localhost')
DEFAULT_PORT = int(os.environ.get('AIOHAPPYBASE_PORT', '9090'))
DEFAULT_COMPAT = os.environ.get('AIOHAPPYBASE_COMPAT', '0.98')
DEFAULT_TRANSPORT = os.environ.get('AIOHAPPYBASE_TRANSPORT', 'buffered')
DEFAULT_PROTOCOL = os.environ.get('AIOHAPPYBASE_PROTOCOL', 'binary')

if parse_version(thriftpy2.__version__) <= parse_version('0.4.10'):
    _make_client = make_client

    def make_client(*args, **kwargs):
        kwargs['socket_timeout'] = kwargs.pop('timeout', 3000)
        return _make_client(*args, **kwargs)


class Connection:
    """
    Connection to an HBase Thrift server.

    The `host` and `port` arguments specify the host name and TCP port
    of the HBase Thrift server to connect to. If omitted or ``None``,
    a connection to the default port on ``localhost`` is made. If
    specifed, the `timeout` argument specifies the socket timeout in
    milliseconds.

    If `autoconnect` is `True` the connection is made directly during
    initialization. Otherwise a context manager should be used (with
    Connection...) or :py:meth:`Connection.open` must be called explicitly
    before first use. Note that due to limitations in the Python async
    framework, a RuntimeError will be raised if it is used inside of a running
    asyncio event loop.

    The optional `table_prefix` and `table_prefix_separator` arguments
    specify a prefix and a separator string to be prepended to all table
    names, e.g. when :py:meth:`Connection.table` is invoked. For
    example, if `table_prefix` is ``myproject``, all tables will
    have names like ``myproject_XYZ``.

    The optional `compat` argument sets the compatibility level for
    this connection. Older HBase versions have slightly different Thrift
    interfaces, and using the wrong protocol can lead to crashes caused
    by communication errors, so make sure to use the correct one. This
    value can be either the string ``0.90``, ``0.92``, ``0.94``, or
    ``0.96`` (the default).

    The optional `transport` argument specifies the Thrift transport
    mode to use. Supported values for this argument are ``buffered``
    (the default) and ``framed``. Make sure to choose the right one,
    since otherwise you might see non-obvious connection errors or
    program hangs when making a connection. HBase versions before 0.94
    always use the buffered transport. Starting with HBase 0.94, the
    Thrift server optionally uses a framed transport, depending on the
    argument passed to the ``hbase-daemon.sh start thrift`` command.
    The default ``-threadpool`` mode uses the buffered transport; the
    ``-hsha``, ``-nonblocking``, and ``-threadedselector`` modes use the
    framed transport.

    The optional `protocol` argument specifies the Thrift transport
    protocol to use. Supported values for this argument are ``binary``
    (the default) and ``compact``. Make sure to choose the right one,
    since otherwise you might see non-obvious connection errors or
    program hangs when making a connection. ``TCompactProtocol`` is
    a more compact binary format that is  typically more efficient to
    process as well. ``TBinaryProtocol`` is the default protocol that
    AIOHappyBase uses.

    .. versionadded:: 0.9
       `protocol` argument

    .. versionadded:: 0.5
       `timeout` argument

    .. versionadded:: 0.4
       `table_prefix_separator` argument

    .. versionadded:: 0.4
       support for framed Thrift transports
    """
    # TODO: Auto generate these?
    THRIFT_TRANSPORTS = dict(
        buffered=TAsyncBufferedTransportFactory(),
        framed=TAsyncFramedTransportFactory(),
    )
    THRIFT_PROTOCOLS = dict(
        binary=TAsyncBinaryProtocolFactory(decode_response=False),
        compact=TAsyncCompactProtocolFactory(decode_response=False),
    )
    THRIFT_CLIENT_FACTORY = staticmethod(make_client)

    def __init__(self,
                 host: str = DEFAULT_HOST,
                 port: int = DEFAULT_PORT,
                 timeout: int = None,
                 autoconnect: bool = False,
                 table_prefix: AnyStr = None,
                 table_prefix_separator: AnyStr = b'_',
                 compat: str = DEFAULT_COMPAT,
                 transport: str = DEFAULT_TRANSPORT,
                 protocol: str = DEFAULT_PROTOCOL,
                 **client_kwargs: Any):
        """
        :param host: The host to connect to
        :param port: The port to connect to
        :param timeout: The socket timeout in milliseconds (optional)
        :param autoconnect: Whether the connection should be opened directly
        :param table_prefix: Prefix used to construct table names (optional)
        :param table_prefix_separator: Separator used for `table_prefix`
        :param compat: Compatibility mode (optional)
        :param transport: Thrift transport mode (optional)
        :param protocol: Thrift protocol mode (optional)
        :param client_kwargs:
            Extra keyword arguments for `make_client()`. See the ThriftPy2
            documentation for more information.
        """
        if table_prefix is not None:
            if not isinstance(table_prefix, (str, bytes)):
                raise TypeError("'table_prefix' must be a string")
            table_prefix = ensure_bytes(table_prefix)

        if not isinstance(table_prefix_separator, (str, bytes)):
            raise TypeError("'table_prefix_separator' must be a string")
        table_prefix_separator = ensure_bytes(table_prefix_separator)

        check_invalid_items(
            compat=(compat, COMPAT_MODES),
            transport=(transport, self.THRIFT_TRANSPORTS),
            protocol=(protocol, self.THRIFT_PROTOCOLS),
        )

        # Allow host and port to be None, which may be easier for
        # applications wrapping a Connection instance.
        self.host = host or DEFAULT_HOST
        self.port = port or DEFAULT_PORT
        self.timeout = timeout
        self.table_prefix = table_prefix
        self.table_prefix_separator = table_prefix_separator
        self.compat = compat

        self._transport_factory = self.THRIFT_TRANSPORTS[transport]
        self._protocol_factory = self.THRIFT_PROTOCOLS[protocol]

        self.client_kwargs = {
            'service': Hbase,
            'host': self.host,
            'port': self.port,
            'timeout': self.timeout,
            'trans_factory': self._transport_factory,
            'proto_factory': self._protocol_factory,
            **client_kwargs,
        }
        self.client = None

        if autoconnect:
            self._autoconnect()

    def _autoconnect(self):
        run_coro(self.open(), "Cannot autoconnect in a running event loop!")

    def _table_name(self, name: AnyStr) -> bytes:
        """Construct a table name by optionally adding a table name prefix."""
        name = ensure_bytes(name)
        if self.table_prefix is None:
            return name
        return self.table_prefix + self.table_prefix_separator + name

    async def open(self) -> None:
        """
        Create and open the underlying client to the HBase instance. This
        method can safely be called more than once.
        """
        if self.client is not None:
            return  # _refresh_thrift_client opened the transport

        logger.debug(f"Opening Thrift transport to {self.host}:{self.port}")
        self.client = await self.THRIFT_CLIENT_FACTORY(**self.client_kwargs)

    def close(self) -> None:
        """
        Close the underlying client to the HBase instance. This method
        can be safely called more than once. Note that the client is
        destroyed after it is closed which will cause errors to occur
        if it is used again before reopening. The :py:class:`Connection`
        can be reopened by calling :py:meth:`open` again.
        """
        if self.client is None:
            return

        if logger is not None:
            # If called from __del__(), module variables may no longer exist.
            logger.debug(f"Closing Thrift transport to {self.host}:{self.port}")

        self.client.close()
        self.client = None

    def table(self, name: AnyStr, use_prefix: bool = True) -> Table:
        """
        Return a table object.

        Returns a :py:class:`happybase.Table` instance for the table
        named `name`. This does not result in a round-trip to the
        server, and the table is not checked for existence.

        The optional `use_prefix` argument specifies whether the table
        prefix (if any) is prepended to the specified `name`. Set this
        to `False` if you want to use a table that resides in another
        ‘prefix namespace’, e.g. a table from a ‘friendly’ application
        co-hosted on the same HBase instance. See the `table_prefix`
        argument to the :py:class:`Connection` constructor for more
        information.

        :param name: the name of the table
        :param use_prefix: whether to use the table prefix (if any)
        :return: Table instance
        """
        name = ensure_bytes(name)
        if use_prefix:
            name = self._table_name(name)
        return Table(name, self)

    # Table administration and maintenance

    async def tables(self) -> List[bytes]:
        """
        Return a list of table names available in this HBase instance.

        If a `table_prefix` was set for this :py:class:`Connection`, only
        tables that have the specified prefix will be listed.

        :return: The table names
        """
        names = await self.client.getTableNames()

        # Filter using prefix, and strip prefix from names
        if self.table_prefix is not None:
            prefix = self._table_name(b'')
            offset = len(prefix)
            names = [n[offset:] for n in names if n.startswith(prefix)]

        return names

    async def create_table(self,
                           name: AnyStr,
                           families: Dict[str, Dict[str, Any]]) -> Table:
        """
        Create a table.

        :param name: The table name
        :param families: The name and options for each column family
        :return: The created table instance

        The `families` argument is a dictionary mapping column family
        names to a dictionary containing the options for this column
        family, e.g.

        ::

            families = {
                'cf1': dict(max_versions=10),
                'cf2': dict(max_versions=1, block_cache_enabled=False),
                'cf3': dict(),  # use defaults
            }
            connection.create_table('mytable', families)

        These options correspond to the ColumnDescriptor structure in
        the Thrift API, but note that the names should be provided in
        Python style, not in camel case notation, e.g. `time_to_live`,
        not `timeToLive`. The following options are supported:

        * ``max_versions`` (`int`)
        * ``compression`` (`str`)
        * ``in_memory`` (`bool`)
        * ``bloom_filter_type`` (`str`)
        * ``bloom_filter_vector_size`` (`int`)
        * ``bloom_filter_nb_hashes`` (`int`)
        * ``block_cache_enabled`` (`bool`)
        * ``time_to_live`` (`int`)
        """
        name = self._table_name(name)
        if not isinstance(families, dict):
            raise TypeError("'families' arg must be a dictionary")

        if not families:
            raise ValueError(f"No column families given for table: {name!r}")

        column_descriptors = []
        for cf_name, options in families.items():
            kwargs = {
                snake_to_camel_case(option_name): value
                for option_name, value in (options or {}).items()
            }

            if not cf_name.endswith(':'):
                cf_name += ':'
            kwargs['name'] = cf_name

            column_descriptors.append(ColumnDescriptor(**kwargs))

        await self.client.createTable(name, column_descriptors)
        return self.table(name, use_prefix=False)

    async def delete_table(self, name: AnyStr, disable: bool = False) -> None:
        """
        Delete the specified table.

        .. versionadded:: 0.5
           `disable` argument

        In HBase, a table always needs to be disabled before it can be
        deleted. If the `disable` argument is `True`, this method first
        disables the table if it wasn't already and then deletes it.

        :param name: The table name
        :param disable: Whether to first disable the table if needed
        """
        if disable and await self.is_table_enabled(name):
            await self.disable_table(name)

        await self.client.deleteTable(self._table_name(name))

    async def enable_table(self, name: AnyStr) -> None:
        """
        Enable the specified table.

        :param name: The table name
        """
        await self.client.enableTable(self._table_name(name))

    async def disable_table(self, name: AnyStr) -> None:
        """
        Disable the specified table.

        :param name: The table name
        """
        await self.client.disableTable(self._table_name(name))

    async def is_table_enabled(self, name: AnyStr) -> None:
        """
        Return whether the specified table is enabled.

        :param str name: The table name

        :return: whether the table is enabled
        :rtype: bool
        """
        return await self.client.isTableEnabled(self._table_name(name))

    async def compact_table(self, name: AnyStr, major: bool = False) -> None:
        """Compact the specified table.

        :param str name: The table name
        :param bool major: Whether to perform a major compaction.
        """
        name = self._table_name(name)
        if major:
            await self.client.majorCompact(name)
        else:
            await self.client.compact(name)

    # Support async context usage
    async def __aenter__(self) -> 'Connection':
        await self.open()
        return self

    async def __aexit__(self, *_exc) -> None:
        self.close()

    # Support context usage
    def __enter__(self) -> 'Connection':
        run_coro(self.open(), error="Use 'async with' in a running event loop!")
        return self

    def __exit__(self, *_exc) -> None:
        self.close()

    def __del__(self) -> None:
        try:
            if self.client._iprot.trans.is_open():  # noqa
                logger.warning(f"{self} was not closed!")
        except:  # noqa
            pass
