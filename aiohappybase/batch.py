"""
AIOHappyBase Batch module.
"""

import logging
import abc
from typing import TYPE_CHECKING, Dict, List, Iterable, Type
from functools import partial
from collections import defaultdict
from numbers import Integral

from Hbase_thrift import BatchMutation, Mutation, TIncrement

if TYPE_CHECKING:
    from .table import Table  # Avoid circular import

logger = logging.getLogger(__name__)


class Batcher(abc.ABC):
    """
    Generic base class for management of batch operations. Implementations
    should handle buffering operations, converting the operations to
    Thrift data structures, and sending the data to the server.

    :py:class:`Batch` will wrap multiple :py:class:`Batcher` instances and
    expose their mutation methods as one API.
    """

    def __init__(self, table: 'Table', batch_size: int = None):
        self._table = table
        self._batch_size = batch_size
        self._reset()

    @abc.abstractmethod
    def _reset(self):
        """Clear any queued data to send."""

    @property
    @abc.abstractmethod
    def _batch_count(self) -> int:
        """Current size of batch for comparison with :py:attr:`_batch_size`"""

    @abc.abstractmethod
    async def send(self):
        """Send the current batch up to the server and clear local data."""

    async def _check_send(self):
        """Call :py:meth:`send` if :py:attr:`_batch_size` has been exceeded."""
        if self._batch_size and self._batch_count >= self._batch_size:
            await self.send()


class MutationBatcher(Batcher):
    """
    Batcher implementation for handling column mutations via the
   `mutateRows` and `mutateRowsTs` HBase Thrift endpoints.
    """

    def __init__(self,
                 table: 'Table',
                 timestamp: int = None,
                 batch_size: int = None,
                 wal: bool = True):
        super().__init__(table, batch_size)
        self._timestamp = timestamp
        self._wal = wal
        self._families = None

        # Save mutator partial here to avoid the if check each time
        if self._timestamp is None:
            mut_rows = self._table.client.mutateRows
        else:
            mut_rows = partial(
                self._table.client.mutateRowsTs,
                timestamp=self._timestamp,
            )
        # Add standard arguments
        self._mutate_rows = partial(mut_rows, self._table.name, attributes={})

    def _reset(self):
        self._mutations = defaultdict(list)
        self._mutation_count = 0

    @property
    def _batch_count(self) -> int:
        return self._mutation_count

    async def send(self) -> None:
        bms = [BatchMutation(row, m) for row, m in self._mutations.items()]
        if not bms:
            return

        logger.debug(
            f"Sending batch for '{self._table.name}' ({self._mutation_count} "
            f"mutations on {len(bms)} rows)"
        )

        await self._mutate_rows(bms)
        self._reset()

    async def put(self,
                  row: bytes,
                  data: Dict[bytes, bytes],
                  wal: bool = None) -> None:
        """
        Store data in the table.

        See :py:meth:`Table.put` for a description of the `row`, `data`,
        and `wal` arguments. The `wal` argument should normally not be
        used; its only use is to override the batch-wide value passed to
        :py:meth:`Table.batch`.
        """
        if wal is None:
            wal = self._wal

        await self._add_mutations(row, [
            Mutation(isDelete=False, column=column, value=value, writeToWAL=wal)
            for column, value in data.items()
        ])

    async def delete(self,
                     row: bytes,
                     columns: Iterable[bytes] = None,
                     wal: bool = None) -> None:
        """
        Delete data from the table.

        See :py:meth:`Table.put` for a description of the `row`, `data`,
        and `wal` arguments. The `wal` argument should normally not be
        used; its only use is to override the batch-wide value passed to
        :py:meth:`Table.batch`.
        """
        # Work-around Thrift API limitation: the mutation API can only
        # delete specified columns, not complete rows, so just list the
        # column families once and cache them for later use by the same
        # batch instance.
        if columns is None:
            if self._families is None:
                self._families = await self._table.column_family_names()
            columns = self._families

        if wal is None:
            wal = self._wal

        await self._add_mutations(row, [
            Mutation(isDelete=True, column=column, writeToWAL=wal)
            for column in columns
        ])

    async def _add_mutations(self, row: bytes, mutations: List[Mutation]):
        self._mutations[row].extend(mutations)
        self._mutation_count += len(mutations)
        await self._check_send()


class CounterBatcher(Batcher):
    """
    Batcher implementation for handling counter manipulations via the
   `incrementRows` HBase Thrift endpoint.
    """

    def _reset(self):
        self._counters = defaultdict(int)

    @property
    def _batch_count(self) -> int:
        return len(self._counters)

    async def counter_inc(self,
                          row: bytes,
                          column: bytes,
                          value: int = 1) -> None:
        """
        Atomically increment (or decrements) a counter column.

        See :py:meth:`Table.counter_inc` for parameter details. Note that
        this method cannot return the current value because the change
        is buffered until send to the server.
        """
        self._counters[(row, column)] += value
        await self._check_send()

    async def counter_dec(self,
                          row: bytes,
                          column: bytes,
                          value: int = 1) -> None:
        """
        Atomically decrement (or increments) a counter column.

        See :py:meth:`Table.counter_dec` for parameter details. Note that
        this method cannot return the current value because the change
        is buffered until send to the server.
        """
        await self.counter_inc(row, column, -value)

    async def send(self) -> None:
        increments = [
            TIncrement(self._table.name, row, col, val)
            for (row, col), val in self._counters.items()
            if val != 0
        ]
        if not increments:
            return
        await self._table.client.incrementRows(increments)
        self._reset()


class Batch:
    """
    Batch mutation class.

    This class cannot be instantiated directly;
    use :py:meth:`Table.batch` instead.
    """
    def __init__(self,
                 table: 'Table',
                 timestamp: int = None,
                 batch_size: int = None,
                 transaction: bool = False,
                 wal: bool = True):
        """Initialise a new Batch instance."""
        if not (timestamp is None or isinstance(timestamp, Integral)):
            raise TypeError("'timestamp' must be an integer or None")

        if batch_size is not None:
            if transaction:
                raise TypeError("'transaction' can't be used with 'batch_size'")
            if not batch_size > 0:
                raise ValueError("'batch_size' must be > 0")

        self._table = table
        self._batch_size = batch_size
        self._transaction = transaction

        self._mutations = MutationBatcher(table, timestamp, batch_size, wal)
        # Expose mutation methods
        self.put = self._mutations.put
        self.delete = self._mutations.delete

        self._counters = CounterBatcher(table, batch_size)
        # Expose counter methods
        self.counter_inc = self._counters.counter_inc
        self.counter_dec = self._counters.counter_dec

    async def send(self) -> None:
        """Send the batch to the server."""
        await self._mutations.send()
        await self._counters.send()

    async def close(self) -> None:
        """Finalize the batch and make sure all tasks are completed."""
        await self.send()  # Send any remaining mutations

    # Support usage as an async context manager
    async def __aenter__(self) -> 'Batch':
        """Called upon entering a ``async with`` block"""
        return self

    async def __aexit__(self, exc_type: Type[Exception], *_) -> None:
        """Called upon exiting a ``async with`` block"""
        # If the 'with' block raises an exception, the batch will not be
        # sent to the server.
        if self._transaction and exc_type is not None:
            return

        await self.close()

    # Guard against porting mistakes
    def __enter__(self):  # pragma: no cover
        raise RuntimeError("Use async with")

    def __exit__(self, *_exc):  # pragma: no cover
        raise RuntimeError("Use async with")
