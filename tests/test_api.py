"""
HappyBase tests.
"""

import gc
import random
import asyncio as aio
import logging
from functools import wraps, partial
from typing import AsyncGenerator

import asynctest

from thriftpy2.thrift import TException

from aiohappybase import Connection, ConnectionPool, NoConnectionsAvailable
from aiohappybase.pool import current_task  # Easiest way to get the right one

TABLE_PREFIX = b'happybase_tests_tmp'
TEST_TABLE_NAME = b'test1'

connection_kwargs = {'table_prefix': TABLE_PREFIX}


def with_new_loop(func):
    @wraps(func)
    def _wrapper(*args, **kwargs):
        old_loop = aio.get_event_loop()
        loop = aio.new_event_loop()
        aio.set_event_loop(loop)
        try:
            return func(*args, loop=loop, **kwargs)
        finally:
            aio.set_event_loop(old_loop)
            loop.close()
    return _wrapper


class TestAPI(asynctest.TestCase):

    @classmethod
    @with_new_loop
    def setUpClass(cls, loop):
        loop.run_until_complete(cls.create_table())

    @classmethod
    @with_new_loop
    def tearDownClass(cls, loop):
        loop.run_until_complete(cls.destroy_table())

    @classmethod
    async def create_table(cls):
        async with Connection(**connection_kwargs) as conn:
            assert conn is not None

            tables = await conn.tables()
            if TEST_TABLE_NAME in tables:  # pragma: no cover
                print("Test table already exists; removing it...")
                await conn.delete_table(TEST_TABLE_NAME, disable=True)

            cfs = {
                'cf1': {},
                'cf2': None,
                'cf3': {'max_versions': 1},
            }
            table = await conn.create_table(TEST_TABLE_NAME, families=cfs)
            assert table is not None

    @classmethod
    async def destroy_table(cls):
        async with Connection(**connection_kwargs) as conn:
            await conn.delete_table(TEST_TABLE_NAME, disable=True)

    async def setUp(self):
        self.connection = Connection(**connection_kwargs)
        await self.connection.open()
        self.table = self.connection.table(TEST_TABLE_NAME)

    def tearDown(self):
        self.connection.close()
        del self.connection
        del self.table

    async def _scan_list(self, *args, **kwargs):
        return [x async for x in self.table.scan(*args, **kwargs)]

    async def _scan_len(self, scanner: AsyncGenerator = None, **kwargs) -> int:
        if scanner is None:
            scanner = self.table.scan(**kwargs)
        return sum([1 async for _ in scanner])

    def test_autoconnect(self):
        conn = Connection(**connection_kwargs, autoconnect=True)
        self.assertTrue(conn.client._iprot.trans.is_open())
        conn.close()

    async def test_double_close(self):
        conn = Connection(**connection_kwargs)
        await conn.open()
        conn.close()
        conn.close()  # No error on second close

    async def test_no_close_warning(self):
        conn = Connection(**connection_kwargs)
        await conn.open()
        client = conn.client  # Save so we can close later
        with self.assertLogs(level=logging.WARNING):
            del conn
            gc.collect()
        client.close()

    def test_connection_invalid_table_prefix(self):
        with self.assertRaises(TypeError):
            Connection(table_prefix=1)  # noqa Not a string

    def test_connection_invalid_table_prefix_sep(self):
        with self.assertRaises(TypeError):
            Connection(table_prefix_separator=1)  # noqa Not a string

    def test_connection_invalid_transport(self):
        with self.assertRaises(ValueError):
            Connection(transport='millennium')

    def test_connection_invalid_protocol(self):
        with self.assertRaises(ValueError):
            Connection(protocol='falcon')

    def test_connection_invalid_compat(self):
        with self.assertRaises(ValueError):
            Connection(compat='0.1.invalid.version')

    async def test_enabling(self):
        conn = self.connection
        self.assertTrue(await conn.is_table_enabled(TEST_TABLE_NAME))
        await conn.disable_table(TEST_TABLE_NAME)
        self.assertFalse(await conn.is_table_enabled(TEST_TABLE_NAME))
        await conn.enable_table(TEST_TABLE_NAME)
        self.assertTrue(await conn.is_table_enabled(TEST_TABLE_NAME))

    async def test_compaction(self):
        await self.connection.compact_table(TEST_TABLE_NAME)
        await self.connection.compact_table(TEST_TABLE_NAME, major=True)

    async def test_prefix(self):
        conn = self.connection
        self.assertEqual(TABLE_PREFIX + b'_', conn._table_name(''))
        self.assertEqual(TABLE_PREFIX + b'_foo', conn._table_name('foo'))

        self.assertEqual(conn.table('foobar').name, TABLE_PREFIX + b'_foobar')
        self.assertEqual(conn.table('foobar', use_prefix=False).name, b'foobar')

        c = Connection(autoconnect=False)  # sync has it set to True
        self.assertEqual(b'foo', c._table_name('foo'))

        with self.assertRaises(TypeError):
            Connection(table_prefix=123)  # noqa

        with self.assertRaises(TypeError):
            Connection(table_prefix_separator=2.1)  # noqa

    async def test_stringify(self):
        str(self.connection)
        repr(self.connection)
        str(self.table)
        repr(self.table)

    async def test_table_listing(self):
        names = await self.connection.tables()
        self.assertIsInstance(names, list)
        self.assertIn(TEST_TABLE_NAME, names)

    async def test_table_regions(self):
        regions = await self.table.regions()
        self.assertIsInstance(regions, list)

    async def test_invalid_table_create(self):
        create_table = partial(self.connection.create_table, 'sometable')
        with self.assertRaises(ValueError):
            await create_table(families={})
        for fam in [0, []]:
            with self.assertRaises(TypeError):
                await create_table(families=fam)  # noqa

    async def test_families(self):
        families = await self.table.families()
        for name, fdesc in families.items():
            self.assertIsInstance(name, bytes)
            self.assertIsInstance(fdesc, dict)
            self.assertIn('name', fdesc)
            self.assertIsInstance(fdesc['name'], bytes)
            self.assertIn('max_versions', fdesc)

    async def test_put(self):
        await self.table.put(b'r1', {b'cf1:c1': b'v1',
                                     b'cf1:c2': b'v2',
                                     b'cf2:c3': b'v3'})
        await self.table.put(b'r1', {b'cf1:c4': b'v2'}, timestamp=2345678)
        await self.table.put(b'r1', {b'cf1:c4': b'v2'}, timestamp=1369168852994)

    async def test_atomic_counters(self):
        row = b'row-with-counter'
        column = b'cf1:counter'

        get = partial(self.table.counter_get, row, column)
        inc = partial(self.table.counter_inc, row, column)
        dec = partial(self.table.counter_dec, row, column)

        self.assertEqual(0, await get())

        self.assertEqual(10, await inc(10))
        self.assertEqual(10, await get())

        await self.table.counter_set(row, column, 0)
        self.assertEqual(1, await inc())
        self.assertEqual(4, await inc(3))
        self.assertEqual(4, await get())

        await self.table.counter_set(row, column, 3)
        self.assertEqual(3, await get())
        self.assertEqual(8, await inc(5))
        self.assertEqual(6, await inc(-2))
        self.assertEqual(5, await dec())
        self.assertEqual(3, await dec(2))
        self.assertEqual(10, await dec(-7))

    async def test_batch(self):
        with self.assertRaises(TypeError):
            self.table.batch(timestamp='invalid')  # noqa

        b = self.table.batch()
        await b.put(b'row1', {b'cf1:col1': b'value1',
                              b'cf1:col2': b'value2'})
        await b.put(b'row2', {b'cf1:col1': b'value1',
                              b'cf1:col2': b'value2',
                              b'cf1:col3': b'value3'})
        await b.delete(b'row1', [b'cf1:col4'])
        await b.delete(b'another-row')
        await b.close()

        self.table.batch(timestamp=1234567)
        await b.put(b'row1', {b'cf1:col5': b'value5'})
        await b.close()

        with self.assertRaises(ValueError):
            self.table.batch(batch_size=0)

        with self.assertRaises(TypeError):
            self.table.batch(transaction=True, batch_size=10)

    async def test_batch_context_managers(self):
        async with self.table.batch() as b:
            await b.put(b'row4', {b'cf1:col3': b'value3'})
            await b.put(b'row5', {b'cf1:col4': b'value4'})
            await b.put(b'row', {b'cf1:col1': b'value1'})
            await b.delete(b'row', [b'cf1:col4'])
            await b.put(b'row', {b'cf1:col2': b'value2'})

        async with self.table.batch(timestamp=87654321) as b:
            await b.put(b'row', {b'cf1:c3': b'somevalue',
                                 b'cf1:c5': b'anothervalue'})
            await b.delete(b'row', [b'cf1:c3'])

        with self.assertRaises(ValueError):
            async with self.table.batch(transaction=True) as b:
                await b.put(b'fooz', {b'cf1:bar': b'baz'})
                raise ValueError
        self.assertDictEqual({}, await self.table.row(b'fooz', [b'cf1:bar']))

        with self.assertRaises(ValueError):
            async with self.table.batch(transaction=False) as b:
                await b.put(b'fooz', {b'cf1:bar': b'baz'})
                raise ValueError
        self.assertDictEqual(
            {b'cf1:bar': b'baz'},
            await self.table.row(b'fooz', [b'cf1:bar']),
        )

        async with self.table.batch(batch_size=5) as b:
            for i in range(10):
                await b.put(
                    f'row-batch1-{i:03}'.encode('ascii'),
                    {b'cf1:': str(i).encode('ascii')},
                )

        async with self.table.batch(batch_size=20) as b:
            for i in range(95):
                await b.put(
                    f'row-batch2-{i:03}'.encode('ascii'),
                    {b'cf1:': str(i).encode('ascii')},
                )
        self.assertEqual(95, await self._scan_len(row_prefix=b'row-batch2-'))

        async with self.table.batch(batch_size=20) as b:
            for i in range(95):
                await b.delete(f'row-batch2-{i:03}'.encode('ascii'))
        self.assertEqual(0, await self._scan_len(row_prefix=b'row-batch2-'))

    async def test_batch_order(self):
        row = b'row-test-batch-order'
        col = b'cf1:col'

        async with self.table.batch() as b:
            for i in range(5):
                await b.put(row, {col: str(i).encode()})
        self.assertEqual((await self.table.row(row))[col], b'4')

    async def test_batch_delete_put_same_row(self):
        # See https://github.com/python-happybase/happybase/issues/224
        row = b'row-test-batch-delete-put'
        col = b'cf1:col'
        val = b'val'

        await self.table.put(row, {col: b''})
        async with self.table.batch() as b:
            await b.delete(row)
            await b.put(row, {col: val})
        result = await self.table.row(row)
        self.assertIn(col, result)
        self.assertEqual(result[col], val)

    async def test_batch_counters(self):
        row = b'row-with-counter'
        col1 = b'cf1:counter1'
        col2 = b'cf1:counter2'

        get = partial(self.table.counter_get, row)

        async def check_cols(c1: int, c2: int):
            for col, val in [(col1, c1), (col2, c2)]:
                self.assertEqual(await get(col), val)

        async with self.table.batch() as b:
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
            await self.table.counter_set(row, c, 0)
        await check_cols(0, 0)

        async with self.table.batch(batch_size=2) as b:
            inc = partial(b.counter_inc, row)
            await inc(col1, 1)
            await check_cols(0, 0)  # Not sent yet
            await inc(col1, 1)
            await check_cols(0, 0)  # Same column modified twice, not sent
            await inc(col2, 1)  # Forces send since batch count >= 2
            await check_cols(2, 1)

    async def test_row(self):
        row = self.table.row
        put = self.table.put
        row_key = b'row-test'

        with self.assertRaises(TypeError):
            await row(row_key, 123)  # noqa

        with self.assertRaises(TypeError):
            await row(row_key, timestamp='invalid')  # noqa

        await put(row_key, {b'cf1:col1': b'v1old'}, timestamp=1234)
        await put(row_key, {b'cf1:col1': b'v1new'}, timestamp=3456)
        await put(row_key, {b'cf1:col2': b'v2', b'cf2:col1': b'v3'})
        await put(row_key, {b'cf2:col2': b'v4'}, timestamp=1234)

        exp = {b'cf1:col1': b'v1new',
               b'cf1:col2': b'v2',
               b'cf2:col1': b'v3',
               b'cf2:col2': b'v4'}
        self.assertDictEqual(exp, await row(row_key))

        exp = {b'cf1:col1': b'v1new', b'cf1:col2': b'v2'}
        self.assertDictEqual(exp, await row(row_key, [b'cf1']))

        exp = {b'cf1:col1': b'v1new', b'cf2:col2': b'v4'}
        self.assertDictEqual(exp, await row(row_key, list(exp)))

        exp = {b'cf1:col1': b'v1old', b'cf2:col2': b'v4'}
        self.assertDictEqual(exp, await row(row_key, timestamp=2345))

        self.assertDictEqual({}, await row(row_key, timestamp=123))

        res = await row(row_key, include_timestamp=True)
        self.assertEqual(len(res), 4)
        self.assertEqual(b'v1new', res[b'cf1:col1'][0])
        self.assertIsInstance(res[b'cf1:col1'][1], int)

    async def test_rows(self):
        row_keys = [b'rows-row1', b'rows-row2', b'rows-row3']
        data_old = {b'cf1:col1': b'v1old', b'cf1:col2': b'v2old'}
        data_new = {b'cf1:col1': b'v1new', b'cf1:col2': b'v2new'}

        with self.assertRaises(TypeError):
            await self.table.rows(row_keys, object())  # noqa

        with self.assertRaises(TypeError):
            await self.table.rows(row_keys, timestamp='invalid')  # noqa

        for row_key in row_keys:
            await self.table.put(row_key, data_old, timestamp=4000)

        for row_key in row_keys:
            await self.table.put(row_key, data_new)

        self.assertDictEqual({}, dict(await self.table.rows([])))

        rows = dict(await self.table.rows(row_keys))
        for row_key in row_keys:
            self.assertIn(row_key, rows)
            self.assertDictEqual(data_new, rows[row_key])

        rows = dict(await self.table.rows(row_keys, timestamp=5000))
        for row_key in row_keys:
            self.assertIn(row_key, rows)
            self.assertDictEqual(data_old, rows[row_key])

    async def test_cells(self):
        row_key = b'cell-test'
        col = b'cf1:col1'

        await self.table.put(row_key, {col: b'old'}, timestamp=1234)
        await self.table.put(row_key, {col: b'new'})

        with self.assertRaises(TypeError):
            await self.table.cells(row_key, col, versions='invalid')  # noqa

        with self.assertRaises(TypeError):
            await self.table.cells(
                row_key, col,
                versions=3,
                timestamp='invalid',  # noqa
            )

        with self.assertRaises(ValueError):
            await self.table.cells(row_key, col, versions=0)

        results = await self.table.cells(row_key, col, versions=1)
        self.assertEqual(len(results), 1)
        self.assertEqual(b'new', results[0])

        results = await self.table.cells(row_key, col)
        self.assertEqual(len(results), 2)
        self.assertEqual(b'new', results[0])
        self.assertEqual(b'old', results[1])

        results = await self.table.cells(
            row_key, col,
            timestamp=2345,
            include_timestamp=True,
        )
        self.assertEqual(len(results), 1)
        self.assertEqual(b'old', results[0][0])
        self.assertEqual(1234, results[0][1])

    async def test_scan(self):
        with self.assertRaises(TypeError):
            await self._scan_list(row_prefix='foobar', row_start='xyz')

        if self.connection.compat == '0.90':
            with self.assertRaises(NotImplementedError):
                await self._scan_list(filter='foo')

        if self.connection.compat < '0.96':
            with self.assertRaises(NotImplementedError):
                await self._scan_list(sorted_columns=True)

        with self.assertRaises(ValueError):
            await self._scan_list(batch_size=0)

        with self.assertRaises(ValueError):
            await self._scan_list(limit=0)

        with self.assertRaises(ValueError):
            await self._scan_list(scan_batching=0)

        async with self.table.batch() as b:
            for i in range(2000):
                await b.put(f'row-scan-a{i:05}'.encode('ascii'),
                            {b'cf1:col1': b'v1',
                             b'cf1:col2': b'v2',
                             b'cf2:col1': b'v1',
                             b'cf2:col2': b'v2'})
                await b.put(f'row-scan-b{i:05}'.encode('ascii'),
                            {b'cf1:col1': b'v1', b'cf1:col2': b'v2'})

        scanner = self.table.scan(
            row_start=b'row-scan-a00012',
            row_stop=b'row-scan-a00022',
        )
        self.assertEqual(10, await self._scan_len(scanner))

        scanner = self.table.scan(row_start=b'xyz')
        self.assertEqual(0, await self._scan_len(scanner))

        scanner = self.table.scan(row_start=b'xyz', row_stop=b'zyx')
        self.assertEqual(0, await self._scan_len(scanner))

        rows = await self._scan_list(
            row_start=b'row-scan-',
            row_stop=b'row-scan-a999',
            columns=[b'cf1:col1', b'cf2:col2'],
        )
        row_key, row = rows[0]
        self.assertEqual(row_key, b'row-scan-a00000')
        self.assertDictEqual(row, {b'cf1:col1': b'v1', b'cf2:col2': b'v2'})
        self.assertEqual(2000, len(rows))

        scanner = self.table.scan(
            row_prefix=b'row-scan-a',
            batch_size=499,
            limit=1000,
        )
        self.assertEqual(1000, await self._scan_len(scanner))

        scanner = self.table.scan(
            row_prefix=b'row-scan-b',
            batch_size=1,
            limit=10,
        )
        self.assertEqual(10, await self._scan_len(scanner))

        scanner = self.table.scan(
            row_prefix=b'row-scan-b',
            batch_size=5,
            limit=10,
        )
        self.assertEqual(10, await self._scan_len(scanner))

        scanner = self.table.scan(timestamp=123)
        self.assertEqual(0, await self._scan_len(scanner))

        scanner = self.table.scan(row_prefix=b'row', timestamp=123)
        self.assertEqual(0, await self._scan_len(scanner))

        scanner = self.table.scan(batch_size=20)
        await scanner.__anext__()
        await scanner.aclose()
        with self.assertRaises(StopAsyncIteration):
            await scanner.__anext__()

    async def test_scan_sorting(self):
        if self.connection.compat < '0.96':
            return  # not supported

        input_row = {f'cf1:col-{i:03}'.encode('ascii'): b'' for i in range(100)}
        input_key = b'row-scan-sorted'
        await self.table.put(input_key, input_row)

        scan = self.table.scan(row_start=input_key, sorted_columns=True)
        key, row = await scan.__anext__()
        self.assertEqual(key, input_key)
        self.assertListEqual(sorted(input_row.items()), list(row.items()))
        await scan.aclose()

    async def test_scan_reverse(self):

        if self.connection.compat < '0.98':
            with self.assertRaises(NotImplementedError):
                await self._scan_list(reverse=True)
            return

        async with self.table.batch() as b:
            for i in range(2000):
                await b.put(f'row-scan-reverse-{i:04}'.encode('ascii'),
                            {b'cf1:col1': b'v1', b'cf1:col2': b'v2'})

        scan = self.table.scan(row_prefix=b'row-scan-reverse', reverse=True)
        self.assertEqual(2000, await self._scan_len(scan))

        self.assertEqual(10, await self._scan_len(limit=10, reverse=True))

        scan = self.table.scan(
            row_start=b'row-scan-reverse-1999',
            row_stop=b'row-scan-reverse-0000',
            reverse=True,
        )
        key, data = await scan.__anext__()
        self.assertEqual(b'row-scan-reverse-1999', key)

        key, data = [x async for x in scan][-1]
        self.assertEqual(b'row-scan-reverse-0001', key)

    async def test_scan_filter_and_batch_size(self):
        # See issue #54 and #56
        filt = b"SingleColumnValueFilter('cf1','col1',=,'binary:%s',true,true)"

        if self.connection.compat == '0.90':
            with self.assertRaises(NotImplementedError):
                await self._scan_len(filter=filt % b'hello there')
            return

        self.assertEqual(0, await self._scan_len(filter=filt % b'hello there'))

        got_results = False
        async for k, v in self.table.scan(filter=filt % b'v1'):
            got_results = True
            self.assertIsNotNone(next((x for x in v if b'cf1' in x), None))
            self.assertEqual(v[b'cf1:col1'], b'v1')

        self.assertTrue(got_results, msg="No results found for cf1:col1='v1'")

    async def test_append(self):
        row_key = b'row-test-append'
        c1 = b'cf1:col1'
        c2 = b'cf1:col2'
        s1 = b'abc'
        s2 = b'123'

        append = partial(self.table.append, row_key)
        get = partial(self.table.row, row_key)

        if self.connection.compat < '0.98':
            with self.assertRaises(NotImplementedError):
                await append({})
            return

        data = {c1: s1, c2: s2}
        self.assertDictEqual(data, await append(data))
        self.assertDictEqual(data, await get())

        expected = {c1: s1 + s2, c2: s2 + s1}
        self.assertDictEqual(expected, await append({c1: s2, c2: s1}))
        self.assertDictEqual(expected, await get())

        for value in (await append(data, include_timestamp=True)).values():
            self.assertIsInstance(value, tuple)
            self.assertEqual(len(value), 2)
            value, ts = value
            self.assertIsInstance(value, bytes)
            self.assertIsInstance(ts, int)

    async def test_delete(self):
        row_key = b'row-test-delete'
        data = {b'cf1:col1': b'v1',
                b'cf1:col2': b'v2',
                b'cf1:col3': b'v3'}
        await self.table.put(row_key, {b'cf1:col2': b'v2old'}, timestamp=1234)
        await self.table.put(row_key, data)

        await self.table.delete(row_key, [b'cf1:col2'], timestamp=2345)
        cells = await self.table.cells(row_key, b'cf1:col2', versions=2)
        self.assertEqual(1, len(cells))
        self.assertDictEqual(data, await self.table.row(row_key))

        await self.table.delete(row_key, [b'cf1:col1'])
        res = await self.table.row(row_key)
        self.assertNotIn(b'cf1:col1', res)
        self.assertIn(b'cf1:col2', res)
        self.assertIn(b'cf1:col3', res)

        await self.table.delete(row_key, timestamp=12345)
        res = await self.table.row(row_key)
        self.assertIn(b'cf1:col2', res)
        self.assertIn(b'cf1:col3', res)

        await self.table.delete(row_key)
        self.assertDictEqual({}, await self.table.row(row_key))

    async def test_connection_pool_construction(self):
        with self.assertRaises(TypeError):
            ConnectionPool(size='abc')  # noqa

        with self.assertRaises(ValueError):
            ConnectionPool(size=0)

    async def test_connection_pool(self):

        async def run():
            print(f"{self._current_task_name()} starting")

            async def inner_function():
                # Nested connection requests must return the same connection
                async with pool.connection() as another_connection:
                    self.assertIs(connection, another_connection)

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

    async def test_pool_exhaustion(self):

        async def run():
            with self.assertRaises(NoConnectionsAvailable):
                async with pool.connection(timeout=.1) as _connection:
                    self.fail("Connection available???")  # pragma: no cover

        async with ConnectionPool(size=1, **connection_kwargs) as pool:
            async with pool.connection():
                # At this point the only connection is assigned to this task,
                # so another task cannot obtain a connection.
                await self._run_tasks(run)

    async def test_pool_no_close_warning(self):
        pool = ConnectionPool(size=1, **connection_kwargs)
        with self.assertLogs(level=logging.WARNING):
            del pool
            gc.collect()

    @staticmethod
    def _run_tasks(func, count: int = 1):
        return aio.gather(*(func() for _ in range(count)))

    @staticmethod
    def _current_task_name() -> str:
        task_id = hex(id(current_task()))
        return f"Task {task_id}"


class TestSyncInAsync(asynctest.TestCase):

    async def test_sync_autoconnect(self):
        with self.assertRaises(RuntimeError):
            Connection(**connection_kwargs, autoconnect=True)

    def test_sync_connection_context(self):
        with Connection(**connection_kwargs) as conn:
            self.assertTrue(conn.client._iprot.trans.is_open())

    async def test_sync_connection_context_while_running(self):
        with self.assertRaises(RuntimeError):
            with Connection(**connection_kwargs):
                pass

    def test_sync_pool_context(self):
        with ConnectionPool(size=1, **connection_kwargs) as pool:
            self.assertIsNotNone(pool)

    async def test_sync_pool_context_while_running(self):
        # Get pool first so we can close it after and prevent the warning
        pool = ConnectionPool(size=1, **connection_kwargs)
        with self.assertRaises(RuntimeError):
            with pool:
                pass
        pool.close()

    async def test_sync_batch_context(self):
        async with Connection(**connection_kwargs) as conn:
            table = conn.table(TEST_TABLE_NAME)
            with self.assertRaises(RuntimeError):
                with table.batch():
                    pass
            with self.assertRaises(RuntimeError):
                table.batch().__enter__()
            batch = await table.batch().__aenter__()
            with self.assertRaises(RuntimeError):
                batch.__exit__()


if __name__ == '__main__':
    import sys

    logging.basicConfig(level=logging.DEBUG)

    method_name = f'test_{sys.argv[1]}'
    method = globals()[method_name]
    method()
