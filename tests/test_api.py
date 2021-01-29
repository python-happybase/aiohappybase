"""
HappyBase tests.
"""

import gc
import random
import logging
import asyncio as aio
from functools import partial
from typing import AsyncGenerator, Tuple, List

import pytest

from thriftpy2.thrift import TException

from aiohappybase import (
    Table,
    Connection,
    ConnectionPool,
    NoConnectionsAvailable,
)
from aiohappybase.pool import current_task  # Easiest way to get the right one
from aiohappybase.table import Data

TABLE_PREFIX = b'happybase_tests_tmp'

connection_kwargs = {'table_prefix': TABLE_PREFIX}


@pytest.fixture
async def conn() -> Connection:
    async with Connection(**connection_kwargs) as conn:
        assert conn is not None
        yield conn


@pytest.fixture
async def table(conn: Connection, table_name: bytes) -> Table:
    cfs = {
        'cf1': {},
        'cf2': None,
        'cf3': {'max_versions': 1},
    }
    table = await conn.create_table(table_name, families=cfs)
    assert table is not None
    yield table
    await conn.delete_table(table_name, disable=True)


class TestAPI:

    @pytest.mark.asyncio
    def test_autoconnect(self):
        conn = Connection(**connection_kwargs, autoconnect=True)
        assert conn.client._iprot.trans.is_open()
        conn.close()

    @pytest.mark.asyncio
    async def test_double_close(self):
        conn = Connection(**connection_kwargs)
        await conn.open()
        conn.close()
        conn.close()  # No error on second close

    @pytest.mark.asyncio
    async def test_no_close_warning(self, caplog):
        conn = Connection(**connection_kwargs)
        await conn.open()
        client = conn.client  # Save so we can close later
        with caplog.at_level(level=logging.WARNING):
            del conn
            gc.collect()
        assert "was not closed" in caplog.text
        client.close()

    @pytest.mark.asyncio
    def test_connection_invalid_table_prefix(self):
        with pytest.raises(TypeError):
            Connection(table_prefix=1)  # noqa Not a string

    @pytest.mark.asyncio
    def test_connection_invalid_table_prefix_sep(self):
        with pytest.raises(TypeError):
            Connection(table_prefix_separator=1)  # noqa Not a string

    @pytest.mark.asyncio
    def test_connection_invalid_transport(self):
        with pytest.raises(ValueError):
            Connection(transport='millennium')

    @pytest.mark.asyncio
    def test_connection_invalid_protocol(self):
        with pytest.raises(ValueError):
            Connection(protocol='falcon')

    @pytest.mark.asyncio
    def test_connection_invalid_compat(self):
        with pytest.raises(ValueError):
            Connection(compat='0.1.invalid.version')

    @pytest.mark.asyncio
    @pytest.mark.usefixtures('table')
    async def test_enabling(self, conn: Connection, table_name: bytes):
        assert await conn.is_table_enabled(table_name)
        await conn.disable_table(table_name)
        assert not await conn.is_table_enabled(table_name)
        await conn.enable_table(table_name)
        assert await conn.is_table_enabled(table_name)

    @pytest.mark.asyncio
    @pytest.mark.usefixtures('table')
    async def test_compaction(self, conn: Connection, table_name: bytes):
        await conn.compact_table(table_name)
        await conn.compact_table(table_name, major=True)

    @pytest.mark.asyncio
    async def test_prefix(self, conn: Connection):
        assert TABLE_PREFIX + b'_' == conn._table_name('')
        assert TABLE_PREFIX + b'_foo' == conn._table_name('foo')

        assert conn.table('foobar').name == TABLE_PREFIX + b'_foobar'
        assert conn.table('foobar', use_prefix=False).name == b'foobar'

        c = Connection(autoconnect=False)  # sync has it set to True
        assert b'foo' == c._table_name('foo')

        with pytest.raises(TypeError):
            Connection(table_prefix=123)  # noqa

        with pytest.raises(TypeError):
            Connection(table_prefix_separator=2.1)  # noqa

    @pytest.mark.asyncio
    async def test_stringify(self, conn: Connection, table: Table):
        str(conn)
        repr(conn)
        str(table)
        repr(table)

    @pytest.mark.asyncio
    @pytest.mark.usefixtures('table')
    async def test_table_listing(self, conn: Connection, table_name: bytes):
        names = await conn.tables()
        assert isinstance(names, list)
        assert table_name in names

    @pytest.mark.asyncio
    async def test_table_regions(self, table: Table):
        assert isinstance(await table.regions(), list)

    @pytest.mark.asyncio
    async def test_invalid_table_create(self, conn: Connection):
        create_table = partial(conn.create_table, 'sometable')
        with pytest.raises(ValueError):
            await create_table(families={})
        for fam in [0, []]:
            with pytest.raises(TypeError):
                await create_table(families=fam)  # noqa

    @pytest.mark.asyncio
    async def test_families(self, table: Table):
        families = await table.families()
        for name, fdesc in families.items():
            assert isinstance(name, bytes)
            assert isinstance(fdesc, dict)
            assert 'name' in fdesc
            assert isinstance(fdesc['name'], bytes)
            assert 'max_versions' in fdesc

    @pytest.mark.asyncio
    async def test_put(self, table: Table):
        await table.put(b'r1', {b'cf1:c1': b'v1',
                                b'cf1:c2': b'v2',
                                b'cf2:c3': b'v3'})
        await table.put(b'r1', {b'cf1:c4': b'v2'}, timestamp=2345678)
        await table.put(b'r1', {b'cf1:c4': b'v2'}, timestamp=1369168852994)

    @pytest.mark.asyncio
    async def test_atomic_counters(self, table: Table):
        row = b'row-with-counter'
        column = b'cf1:counter'

        get = partial(table.counter_get, row, column)
        inc = partial(table.counter_inc, row, column)
        dec = partial(table.counter_dec, row, column)

        assert 0 == await get()

        assert 10 == await inc(10)
        assert 10 == await get()

        await table.counter_set(row, column, 0)
        assert 1 == await inc()
        assert 4 == await inc(3)
        assert 4 == await get()

        await table.counter_set(row, column, 3)
        assert 3 == await get()
        assert 8 == await inc(5)
        assert 6 == await inc(-2)
        assert 5 == await dec()
        assert 3 == await dec(2)
        assert 10 == await dec(-7)

    @pytest.mark.asyncio
    async def test_batch(self, table: Table):
        with pytest.raises(TypeError):
            table.batch(timestamp='invalid')  # noqa

        b = table.batch()
        await b.put(b'row1', {b'cf1:col1': b'value1',
                              b'cf1:col2': b'value2'})
        await b.put(b'row2', {b'cf1:col1': b'value1',
                              b'cf1:col2': b'value2',
                              b'cf1:col3': b'value3'})
        await b.delete(b'row1', [b'cf1:col4'])
        await b.delete(b'another-row')
        await b.close()

        table.batch(timestamp=1234567)
        await b.put(b'row1', {b'cf1:col5': b'value5'})
        await b.close()

        with pytest.raises(ValueError):
            table.batch(batch_size=0)

        with pytest.raises(TypeError):
            table.batch(transaction=True, batch_size=10)

    @pytest.mark.asyncio
    async def test_batch_context_managers(self, table: Table):
        async with table.batch() as b:
            await b.put(b'row4', {b'cf1:col3': b'value3'})
            await b.put(b'row5', {b'cf1:col4': b'value4'})
            await b.put(b'row', {b'cf1:col1': b'value1'})
            await b.delete(b'row', [b'cf1:col4'])
            await b.put(b'row', {b'cf1:col2': b'value2'})

        async with table.batch(timestamp=87654321) as b:
            await b.put(b'row', {b'cf1:c3': b'somevalue',
                                 b'cf1:c5': b'anothervalue'})
            await b.delete(b'row', [b'cf1:c3'])

        with pytest.raises(ValueError):
            async with table.batch(transaction=True) as b:
                await b.put(b'fooz', {b'cf1:bar': b'baz'})
                raise ValueError
        assert {} == await table.row(b'fooz', [b'cf1:bar'])

        with pytest.raises(ValueError):
            async with table.batch(transaction=False) as b:
                await b.put(b'fooz', {b'cf1:bar': b'baz'})
                raise ValueError
        assert {b'cf1:bar': b'baz'} == await table.row(b'fooz', [b'cf1:bar'])

        async with table.batch(batch_size=5) as b:
            for i in range(10):
                await b.put(
                    f'row-batch1-{i:03}'.encode('ascii'),
                    {b'cf1:': str(i).encode('ascii')},
                )

        async with table.batch(batch_size=20) as b:
            for i in range(95):
                await b.put(
                    f'row-batch2-{i:03}'.encode('ascii'),
                    {b'cf1:': str(i).encode('ascii')},
                )
        assert 95 == await self._tbl_scan_len(table, row_prefix=b'row-batch2-')

        async with table.batch(batch_size=20) as b:
            for i in range(95):
                await b.delete(f'row-batch2-{i:03}'.encode('ascii'))
        assert 0 == await self._tbl_scan_len(table, row_prefix=b'row-batch2-')

    @pytest.mark.asyncio
    async def test_batch_order(self, table: Table):
        row = b'row-test-batch-order'
        col = b'cf1:col'

        async with table.batch() as b:
            for i in range(5):
                await b.put(row, {col: str(i).encode()})
        assert (await table.row(row))[col] == b'4'

    @pytest.mark.asyncio
    async def test_batch_delete_put_same_row(self, table: Table):
        # See https://github.com/python-happybase/happybase/issues/224
        row = b'row-test-batch-delete-put'
        col = b'cf1:col'
        val = b'val'

        await table.put(row, {col: b''})
        async with table.batch() as b:
            await b.delete(row)
            await b.put(row, {col: val})
        result = await table.row(row)
        assert col in result
        assert result[col] == val

    @pytest.mark.asyncio
    async def test_batch_counters(self, table: Table):
        row = b'row-with-counter'
        col1 = b'cf1:counter1'
        col2 = b'cf1:counter2'

        get = partial(table.counter_get, row)

        async def check_cols(c1: int, c2: int):
            for col, val in [(col1, c1), (col2, c2)]:
                assert await get(col) == val

        async with table.batch() as b:
            inc = partial(b.counter_inc, row)
            dec = partial(b.counter_dec, row)
            await inc(col1, 1)  # c1 == 1, c2 == 0
            await inc(col1, 2)  # c1 == 3, c2 == 0
            await dec(col2, 2)  # c1 == 3, c2 == -2
            await dec(col1, 1)  # c1 == 2, c2 == -2
            await inc(col2, 5)  # c1 == 2, c2 == 3
            # Make sure nothing was sent yet
            await check_cols(0, 0)

        await check_cols(2, 3)

        for c in [col1, col2]:
            await table.counter_set(row, c, 0)
        await check_cols(0, 0)

        async with table.batch(batch_size=2) as b:
            inc = partial(b.counter_inc, row)
            await inc(col1, 1)
            await check_cols(0, 0)  # Not sent yet
            await inc(col1, 1)
            await check_cols(0, 0)  # Same column modified twice, not sent
            await inc(col2, 1)  # Forces send since batch count >= 2
            await check_cols(2, 1)

    @pytest.mark.asyncio
    async def test_row(self, table: Table):
        row = table.row
        put = table.put
        row_key = b'row-test'

        with pytest.raises(TypeError):
            await row(row_key, 123)  # noqa

        with pytest.raises(TypeError):
            await row(row_key, timestamp='invalid')  # noqa

        await put(row_key, {b'cf1:col1': b'v1old'}, timestamp=1234)
        await put(row_key, {b'cf1:col1': b'v1new'}, timestamp=3456)
        await put(row_key, {b'cf1:col2': b'v2', b'cf2:col1': b'v3'})
        await put(row_key, {b'cf2:col2': b'v4'}, timestamp=1234)

        exp = {b'cf1:col1': b'v1new',
               b'cf1:col2': b'v2',
               b'cf2:col1': b'v3',
               b'cf2:col2': b'v4'}
        assert exp == await row(row_key)

        exp = {b'cf1:col1': b'v1new', b'cf1:col2': b'v2'}
        assert exp == await row(row_key, [b'cf1'])

        exp = {b'cf1:col1': b'v1new', b'cf2:col2': b'v4'}
        assert exp == await row(row_key, list(exp))

        exp = {b'cf1:col1': b'v1old', b'cf2:col2': b'v4'}
        assert exp == await row(row_key, timestamp=2345)

        assert {} == await row(row_key, timestamp=123)

        res = await row(row_key, include_timestamp=True)
        assert len(res) == 4
        assert b'v1new' == res[b'cf1:col1'][0]
        assert isinstance(res[b'cf1:col1'][1], int)

    @pytest.mark.asyncio
    async def test_rows(self, table: Table):
        row_keys = [b'rows-row1', b'rows-row2', b'rows-row3']
        data_old = {b'cf1:col1': b'v1old', b'cf1:col2': b'v2old'}
        data_new = {b'cf1:col1': b'v1new', b'cf1:col2': b'v2new'}

        with pytest.raises(TypeError):
            await table.rows(row_keys, object())  # noqa

        with pytest.raises(TypeError):
            await table.rows(row_keys, timestamp='invalid')  # noqa

        for row_key in row_keys:
            await table.put(row_key, data_old, timestamp=4000)

        for row_key in row_keys:
            await table.put(row_key, data_new)

        assert {} == dict(await table.rows([]))

        rows = dict(await table.rows(row_keys))
        for row_key in row_keys:
            assert row_key in rows
            assert data_new == rows[row_key]

        rows = dict(await table.rows(row_keys, timestamp=5000))
        for row_key in row_keys:
            assert row_key in rows
            assert data_old == rows[row_key]

    @pytest.mark.asyncio
    async def test_cells(self, table: Table):
        row_key = b'cell-test'
        col = b'cf1:col1'

        await table.put(row_key, {col: b'old'}, timestamp=1234)
        await table.put(row_key, {col: b'new'})

        with pytest.raises(TypeError):
            await table.cells(row_key, col, versions='invalid')  # noqa

        with pytest.raises(TypeError):
            await table.cells(
                row_key, col,
                versions=3,
                timestamp='invalid',  # noqa
            )

        with pytest.raises(ValueError):
            await table.cells(row_key, col, versions=0)

        results = await table.cells(row_key, col, versions=1)
        assert len(results) == 1
        assert b'new' == results[0]

        results = await table.cells(row_key, col)
        assert len(results) == 2
        assert b'new' == results[0]
        assert b'old' == results[1]

        results = await table.cells(
            row_key, col,
            timestamp=2345,
            include_timestamp=True,
        )
        assert len(results) == 1
        assert b'old' == results[0][0]
        assert 1234 == results[0][1]

    @pytest.mark.asyncio
    async def test_scan(self, table: Table):
        with pytest.raises(TypeError):
            await self._scan_list(table, row_prefix='foobar', row_start='xyz')

        if table.connection.compat == '0.90':
            with pytest.raises(NotImplementedError):
                await self._scan_list(table, filter='foo')

        if table.connection.compat < '0.96':
            with pytest.raises(NotImplementedError):
                await self._scan_list(table, sorted_columns=True)

        with pytest.raises(ValueError):
            await self._scan_list(table, batch_size=0)

        with pytest.raises(ValueError):
            await self._scan_list(table, limit=0)

        with pytest.raises(ValueError):
            await self._scan_list(table, scan_batching=0)

        async with table.batch() as b:
            for i in range(2000):
                await b.put(f'row-scan-a{i:05}'.encode('ascii'),
                            {b'cf1:col1': b'v1',
                             b'cf1:col2': b'v2',
                             b'cf2:col1': b'v1',
                             b'cf2:col2': b'v2'})
                await b.put(f'row-scan-b{i:05}'.encode('ascii'),
                            {b'cf1:col1': b'v1', b'cf1:col2': b'v2'})

        scanner = table.scan(
            row_start=b'row-scan-a00012',
            row_stop=b'row-scan-a00022',
        )
        assert 10 == await self._scan_len(scanner)

        scanner = table.scan(row_start=b'xyz')
        assert 0 == await self._scan_len(scanner)

        scanner = table.scan(row_start=b'xyz', row_stop=b'zyx')
        assert 0 == await self._scan_len(scanner)

        rows = await self._scan_list(
            table,
            row_start=b'row-scan-',
            row_stop=b'row-scan-a999',
            columns=[b'cf1:col1', b'cf2:col2'],
        )
        row_key, row = rows[0]
        assert row_key == b'row-scan-a00000'
        assert row == {b'cf1:col1': b'v1', b'cf2:col2': b'v2'}
        assert 2000 == len(rows)

        scanner = table.scan(
            row_prefix=b'row-scan-a',
            batch_size=499,
            limit=1000,
        )
        assert 1000 == await self._scan_len(scanner)

        scanner = table.scan(
            row_prefix=b'row-scan-b',
            batch_size=1,
            limit=10,
        )
        assert 10 == await self._scan_len(scanner)

        scanner = table.scan(
            row_prefix=b'row-scan-b',
            batch_size=5,
            limit=10,
        )
        assert 10 == await self._scan_len(scanner)

        scanner = table.scan(timestamp=123)
        assert 0 == await self._scan_len(scanner)

        scanner = table.scan(row_prefix=b'row', timestamp=123)
        assert 0 == await self._scan_len(scanner)

        scanner = table.scan(batch_size=20)
        await scanner.__anext__()
        await scanner.aclose()
        with pytest.raises(StopAsyncIteration):
            await scanner.__anext__()

    @pytest.mark.asyncio
    async def test_scan_sorting(self, table: Table):
        if table.connection.compat < '0.96':
            return  # not supported

        input_row = {f'cf1:col-{i:03}'.encode('ascii'): b'' for i in range(100)}
        input_key = b'row-scan-sorted'
        await table.put(input_key, input_row)

        scan = table.scan(row_start=input_key, sorted_columns=True)
        key, row = await scan.__anext__()
        assert key == input_key
        assert sorted(input_row.items()) == list(row.items())
        await scan.aclose()

    @pytest.mark.asyncio
    async def test_scan_reverse(self, table: Table):

        if table.connection.compat < '0.98':
            with pytest.raises(NotImplementedError):
                await self._scan_list(table, reverse=True)
            return

        async with table.batch() as b:
            for i in range(2000):
                await b.put(f'row-scan-reverse-{i:04}'.encode('ascii'),
                            {b'cf1:col1': b'v1', b'cf1:col2': b'v2'})

        scan = table.scan(row_prefix=b'row-scan-reverse', reverse=True)
        assert 2000 == await self._scan_len(scan)

        assert 10 == await self._tbl_scan_len(table, limit=10, reverse=True)

        scan = table.scan(
            row_start=b'row-scan-reverse-1999',
            row_stop=b'row-scan-reverse-0000',
            reverse=True,
        )
        key, data = await scan.__anext__()
        assert b'row-scan-reverse-1999' == key

        key, data = [x async for x in scan][-1]
        assert b'row-scan-reverse-0001' == key

    @pytest.mark.asyncio
    async def test_scan_filter_and_batch_size(self, table: Table):
        # See issue #54 and #56
        filt = b"SingleColumnValueFilter('cf1','col1',=,'binary:%s',true,true)"

        if table.connection.compat == '0.90':
            with pytest.raises(NotImplementedError):
                await self._tbl_scan_len(table, filter=filt % b'hello there')
            return

        assert 0 == await self._tbl_scan_len(table,
                                             filter=filt % b'hello there')

        await table.put(b'row-test-scan-filter', {b'cf1:col1': b'v1'})

        got_results = False
        async for k, v in table.scan(filter=filt % b'v1'):
            got_results = True
            assert next((x for x in v if b'cf1' in x), None) is not None
            assert v[b'cf1:col1'] == b'v1'

        assert got_results, "No results found for cf1:col1='v1'"

    @pytest.mark.asyncio
    async def test_append(self, conn: Connection, table: Table):
        row_key = b'row-test-append'
        c1 = b'cf1:col1'
        c2 = b'cf1:col2'
        s1 = b'abc'
        s2 = b'123'

        append = partial(table.append, row_key)
        get = partial(table.row, row_key)

        if conn.compat < '0.98':
            with pytest.raises(NotImplementedError):
                await append({})
            return

        data = {c1: s1, c2: s2}
        assert data == await append(data)
        assert data == await get()

        expected = {c1: s1 + s2, c2: s2 + s1}
        assert expected == await append({c1: s2, c2: s1})
        assert expected == await get()

        for value in (await append(data, include_timestamp=True)).values():
            assert isinstance(value, tuple)
            assert len(value) == 2
            value, ts = value
            assert isinstance(value, bytes)
            assert isinstance(ts, int)

    @pytest.mark.asyncio
    async def test_delete(self, table: Table):
        row_key = b'row-test-delete'
        data = {b'cf1:col1': b'v1',
                b'cf1:col2': b'v2',
                b'cf1:col3': b'v3'}
        await table.put(row_key, {b'cf1:col2': b'v2old'}, timestamp=1234)
        await table.put(row_key, data)

        await table.delete(row_key, [b'cf1:col2'], timestamp=2345)
        cells = await table.cells(row_key, b'cf1:col2', versions=2)
        assert 1 == len(cells)
        assert data == await table.row(row_key)

        await table.delete(row_key, [b'cf1:col1'])
        res = await table.row(row_key)
        assert b'cf1:col1' not in res
        assert b'cf1:col2' in res
        assert b'cf1:col3' in res

        await table.delete(row_key, timestamp=12345)
        res = await table.row(row_key)
        assert b'cf1:col2' in res
        assert b'cf1:col3' in res

        await table.delete(row_key)
        assert {} == await table.row(row_key)

    @pytest.mark.asyncio
    async def test_connection_pool_construction(self):
        with pytest.raises(TypeError):
            ConnectionPool(size='abc')  # noqa

        with pytest.raises(ValueError):
            ConnectionPool(size=0)

    @pytest.mark.asyncio
    async def test_connection_pool(self):

        async def run():
            print(f"{self._current_task_name()} starting")

            async def inner_function():
                # Nested connection requests must return the same connection
                async with pool.connection() as another_connection:
                    assert connection is another_connection

                    # Fake an exception once in a while
                    if random.random() < .25:
                        print("Introducing random failure")
                        connection.client.close()
                        raise TException("Fake transport exception")

            for i in range(50):
                async with pool.connection() as connection:
                    await connection.tables()

                    try:
                        await inner_function()
                    except TException:
                        # This error should have been picked up by the
                        # connection pool, and the connection should have
                        # been replaced by a fresh one
                        pass

                    await connection.tables()

            print(f"{self._current_task_name()} done")

        async with ConnectionPool(size=3, **connection_kwargs) as pool:
            await self._run_tasks(run, count=10)

    @pytest.mark.asyncio
    async def test_pool_exhaustion(self):

        async def run():
            with pytest.raises(NoConnectionsAvailable):
                async with pool.connection(timeout=.1) as _connection:
                    pytest.fail("Connection available???")  # pragma: no cover

        async with ConnectionPool(size=1, **connection_kwargs) as pool:
            async with pool.connection():
                # At this point the only connection is assigned to this task,
                # so another task cannot obtain a connection.
                await self._run_tasks(run)

    @pytest.mark.asyncio
    async def test_pool_no_close_warning(self, caplog):
        pool = ConnectionPool(size=1, **connection_kwargs)
        with caplog.at_level(level=logging.WARNING):
            del pool
            gc.collect()
        assert "was not closed" in caplog.text

    @staticmethod
    def _run_tasks(func, count: int = 1):
        return aio.gather(*(func() for _ in range(count)))

    @staticmethod
    def _current_task_name() -> str:
        task_id = hex(id(current_task()))
        return f"Task {task_id}"

    @staticmethod
    async def _scan_list(table: Table,
                         *args,
                         **kwargs) -> List[Tuple[bytes, Data]]:
        return [x async for x in table.scan(*args, **kwargs)]

    @classmethod
    async def _tbl_scan_len(cls, table: Table, **kwargs) -> int:
        return await cls._scan_len(table.scan(**kwargs))

    @staticmethod
    async def _scan_len(scanner: AsyncGenerator) -> int:
        return sum([1 async for _ in scanner])


class TestSyncInAsync:

    @pytest.mark.asyncio
    async def test_sync_autoconnect(self):
        with pytest.raises(RuntimeError):
            Connection(**connection_kwargs, autoconnect=True)

    def test_sync_connection_context(self):
        with Connection(**connection_kwargs) as conn:
            assert conn.client._iprot.trans.is_open()

    @pytest.mark.asyncio
    async def test_sync_connection_context_while_running(self):
        with pytest.raises(RuntimeError):
            with Connection(**connection_kwargs):
                pass

    def test_sync_pool_context(self):
        with ConnectionPool(size=1, **connection_kwargs) as pool:
            assert pool is not None

    @pytest.mark.asyncio
    async def test_sync_pool_context_while_running(self):
        # Get pool first so we can close it after and prevent the warning
        pool = ConnectionPool(size=1, **connection_kwargs)
        with pytest.raises(RuntimeError):
            with pool:
                pass
        pool.close()

    @pytest.mark.asyncio
    async def test_sync_batch_context(self, table: Table):
        with pytest.raises(RuntimeError):
            with table.batch():
                pass
        with pytest.raises(RuntimeError):
            table.batch().__enter__()
        batch = await table.batch().__aenter__()
        with pytest.raises(RuntimeError):
            batch.__exit__()


if __name__ == '__main__':
    import sys

    logging.basicConfig(level=logging.DEBUG)

    method_name = f'test_{sys.argv[1]}'
    method = globals()[method_name]
    method()
