"""
AIOHappyBase Batch module.
"""

import abc
import logging
from functools import partial
from collections import defaultdict
from numbers import Integral
from typing import TYPE_CHECKING, Dict, Iterable, Type, Tuple, Optional as Opt

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


class MutationProxy:
    """
    Class to store mutations during batching before they are finalized. To
    signify a delete mutation, :py:attr:`value` should be set to ``None``.
    """
    __slots__ = 'col', 'value', 'wal'

    def __init__(self, col: bytes, value: Opt[bytes], wal: bool):
        self.col = col
        self.value = value
        self.wal = wal

    def update(self, value: Opt[bytes], wal: bool = True):
        """Change the value and wal."""
        self.value = value
        self.wal = wal

    def resolve(self) -> Mutation:
        """Create the Mutation object to send to the server."""
        return Mutation(self.is_delete, self.col, self.value, self.wal)

    @property
    def is_delete(self) -> bool:
        return self.value is None


class MutationBatcher(Batcher):
    """
    Batcher implementation for handling column mutations via the
   `mutateRows` and `mutateRowsTs` HBase Thrift endpoints.
    """
    _mutations: Dict[bytes, Dict[bytes, MutationProxy]]

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
        self._mutations = defaultdict(dict)
        self._mutation_count = 0

    @property
    def _batch_count(self) -> int:
        return self._mutation_count

    async def send(self) -> None:
        await self._handle_delete_collisions()

        # Send the rest of the non-colliding mutations
        batch_mutations = [
            BatchMutation(row, [m.resolve() for m in col_muts.values()])
            for row, col_muts in self._mutations.items()
        ]
        if not batch_mutations:
            return

        logger.debug(
            f"Sending batch for '{self._table.name}' ({self._mutation_count} "
            f"mutations on {len(batch_mutations)} rows)"
        )

        await self._mutate_rows(batch_mutations)
        self._reset()

    async def _handle_delete_collisions(self):
        """
        Fix for: https://github.com/python-happybase/happybase/issues/224

        Deletes are run after puts in the HBase thrift handler. If the
        user deletes a row/cf and then tries to put to it, the delete will
        overwrite the put. Work around this bug by detecting collisions
        and sending the deletes first, if any.

        `Offending code as of writing <https://github.com/apache/hbase/blob
        /b99f58304e9c83f3307acd24feaca90eb629120f/hbase-thrift/src/main/java
        /org/apache/hadoop/hbase/thrift/ThriftHBaseServiceHandler.java#L789>`__
        """
        if self._mutation_count == 1:
            return  # Skip this for single puts/deletes

        colliding_deletes = []

        for row, col_muts in self._mutations.items():
            cf_deletes = set()  # Column families that are deleted
            cf_puts = set()  # Column families that are put to
            for col, mut in col_muts.items():
                fam, _, qf = col.partition(b':')  # Get family and qualifier
                if qf and not mut.is_delete:  # Put to a column
                    cf_puts.add(fam)
                elif not qf and mut.is_delete:  # Delete a whole column family
                    cf_deletes.add(fam)

            # Get the Mutations that delete a family which needs to be put to
            deletes = [col_muts.pop(c).resolve() for c in cf_deletes & cf_puts]
            if deletes:
                colliding_deletes.append(BatchMutation(row, deletes))

        if colliding_deletes:
            logger.debug("Found delete/put collisions. Sending deletes first.")
            await self._mutate_rows(colliding_deletes)

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
        await self._add_mutations(row, wal, data.items())  # noqa

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

        await self._add_mutations(row, wal, ((c, None) for c in columns))

    async def _add_mutations(self,
                             row: bytes,
                             wal: bool,
                             mutations: Iterable[Tuple[bytes, Opt[bytes]]]):
        wal = wal if wal is not None else self._wal
        row_mut = self._mutations[row]
        for col, val in mutations:
            try:
                row_mut[col].update(val, wal)
            except KeyError:
                row_mut[col] = MutationProxy(col, val, wal)
                self._mutation_count += 1
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

    # Show documentation for mutation methods
    put = MutationBatcher.put
    delete = MutationBatcher.delete
    counter_inc = CounterBatcher.counter_inc
    counter_dec = CounterBatcher.counter_dec

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
