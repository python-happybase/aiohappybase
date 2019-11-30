"""
HappyBase tests.
"""

import os
import random
import unittest
from threading import Thread, current_thread
from typing import Generator

from thriftpy2.thrift import TException

from aiohappybase.sync import (
    Connection,
    Table,
    ConnectionPool,
    NoConnectionsAvailable,
)

HAPPYBASE_HOST = os.environ.get('HAPPYBASE_HOST', 'localhost')
HAPPYBASE_PORT = int(os.environ.get('HAPPYBASE_PORT', '9090'))
HAPPYBASE_COMPAT = os.environ.get('HAPPYBASE_COMPAT', '0.98')
HAPPYBASE_TRANSPORT = os.environ.get('HAPPYBASE_TRANSPORT', 'buffered')

TABLE_PREFIX = b'happybase_tests_tmp'
TEST_TABLE_NAME = b'test1'

connection_kwargs = dict(
    host=HAPPYBASE_HOST,
    port=HAPPYBASE_PORT,
    table_prefix=TABLE_PREFIX,
    compat=HAPPYBASE_COMPAT,
    transport=HAPPYBASE_TRANSPORT,
)


class TestAPI(unittest.TestCase):
    connection: Connection
    table: Table

    @classmethod
    def setUpClass(cls):
        with Connection(**connection_kwargs) as conn:
            assert conn is not None

            tables = conn.tables()
            if TEST_TABLE_NAME in tables:
                print("Test table already exists; removing it...")
                conn.delete_table(TEST_TABLE_NAME, disable=True)

            cfs = {
                'cf1': {},
                'cf2': None,
                'cf3': {'max_versions': 1},
            }
            table = conn.create_table(TEST_TABLE_NAME, families=cfs)
            assert table is not None

    @classmethod
    def tearDownClass(cls):
        with Connection(**connection_kwargs) as conn:
            conn.delete_table(TEST_TABLE_NAME, disable=True)

    def setUp(self):
        self.connection = Connection(**connection_kwargs)
        self.connection.open()
        self.table = self.connection.table(TEST_TABLE_NAME)

    def tearDown(self):
        self.connection.close()
        del self.connection
        del self.table

    def _scan_list(self, *args, **kwargs):
        return [x for x in self.table.scan(*args, **kwargs)]

    def _scan_len(self, scanner: Generator = None, **kwargs) -> int:
        if scanner is None:
            scanner = self.table.scan(**kwargs)
        i = 0
        for _ in scanner:
            i += 1
        return i

    def test_connection_compat(self):
        with self.assertRaises(ValueError):
            Connection(compat='0.1.invalid.version', autoconnect=False)

    def test_timeout_arg(self):
        Connection(timeout=5000, autoconnect=False)

    def test_enabling(self):
        conn = self.connection
        self.assertTrue(conn.is_table_enabled(TEST_TABLE_NAME))
        conn.disable_table(TEST_TABLE_NAME)
        self.assertFalse(conn.is_table_enabled(TEST_TABLE_NAME))
        conn.enable_table(TEST_TABLE_NAME)
        self.assertTrue(conn.is_table_enabled(TEST_TABLE_NAME))

    def test_compaction(self):
        self.connection.compact_table(TEST_TABLE_NAME)
        self.connection.compact_table(TEST_TABLE_NAME, major=True)

    def test_prefix(self):
        conn = self.connection
        self.assertEqual(TABLE_PREFIX + b'_', conn._table_name(''))
        self.assertEqual(TABLE_PREFIX + b'_foo', conn._table_name('foo'))

        self.assertEqual(conn.table('foobar').name, TABLE_PREFIX + b'_foobar')
        self.assertEqual(conn.table('foobar', use_prefix=False).name, b'foobar')

        with Connection() as c:
            self.assertEqual(b'foo', c._table_name('foo'))

        with self.assertRaises(TypeError):
            Connection(table_prefix=123, autoconnect=False)  # noqa

        with self.assertRaises(TypeError):
            Connection(table_prefix_separator=2.1, autoconnect=False)  # noqa

    def test_stringify(self):
        str(self.connection)
        repr(self.connection)
        str(self.table)
        repr(self.table)

    def test_table_listing(self):
        names = self.connection.tables()
        self.assertIsInstance(names, list)
        self.assertIn(TEST_TABLE_NAME, names)

    def test_table_regions(self):
        regions = self.table.regions()
        self.assertIsInstance(regions, list)

    def test_invalid_table_create(self):
        with self.assertRaises(ValueError):
            self.connection.create_table('sometable', families={})
        with self.assertRaises(TypeError):
            self.connection.create_table('sometable', families=0)  # noqa
        with self.assertRaises(TypeError):
            self.connection.create_table('sometable', families=[])  # noqa

    def test_families(self):
        families = self.table.families()
        for name, fdesc in families.items():
            self.assertIsInstance(name, bytes)
            self.assertIsInstance(fdesc, dict)
            self.assertIn('name', fdesc)
            self.assertIsInstance(fdesc['name'], bytes)
            self.assertIn('max_versions', fdesc)

    def test_put(self):
        self.table.put(b'r1', {b'cf1:c1': b'v1',
                               b'cf1:c2': b'v2',
                               b'cf2:c3': b'v3'})
        self.table.put(b'r1', {b'cf1:c4': b'v2'}, timestamp=2345678)
        self.table.put(b'r1', {b'cf1:c4': b'v2'}, timestamp=1369168852994)

    def test_atomic_counters(self):
        row = b'row-with-counter'
        column = b'cf1:counter'

        get = self.table.counter_get
        inc = self.table.counter_inc
        dec = self.table.counter_dec

        self.assertEqual(0, get(row, column))

        self.assertEqual(10, inc(row, column, 10))
        self.assertEqual(10, get(row, column))

        self.table.counter_set(row, column, 0)
        self.assertEqual(1, inc(row, column))
        self.assertEqual(4, inc(row, column, 3))
        self.assertEqual(4, get(row, column))

        self.table.counter_set(row, column, 3)
        self.assertEqual(3, get(row, column))
        self.assertEqual(8, inc(row, column, 5))
        self.assertEqual(6, inc(row, column, -2))
        self.assertEqual(5, dec(row, column))
        self.assertEqual(3, dec(row, column, 2))
        self.assertEqual(10, dec(row, column, -7))

    def test_batch(self):
        with self.assertRaises(TypeError):
            self.table.batch(timestamp='invalid')  # noqa

        b = self.table.batch()
        b.put(b'row1', {b'cf1:col1': b'value1',
                        b'cf1:col2': b'value2'})
        b.put(b'row2', {b'cf1:col1': b'value1',
                        b'cf1:col2': b'value2',
                        b'cf1:col3': b'value3'})
        b.delete(b'row1', [b'cf1:col4'])
        b.delete(b'another-row')
        b.close()

        self.table.batch(timestamp=1234567)
        b.put(b'row1', {b'cf1:col5': b'value5'})
        b.close()

        with self.assertRaises(ValueError):
            self.table.batch(batch_size=0)

        with self.assertRaises(TypeError):
            self.table.batch(transaction=True, batch_size=10)

    def test_batch_context_managers(self):
        with self.table.batch() as b:
            b.put(b'row4', {b'cf1:col3': b'value3'})
            b.put(b'row5', {b'cf1:col4': b'value4'})
            b.put(b'row', {b'cf1:col1': b'value1'})
            b.delete(b'row', [b'cf1:col4'])
            b.put(b'row', {b'cf1:col2': b'value2'})

        with self.table.batch(timestamp=87654321) as b:
            b.put(b'row', {b'cf1:c3': b'somevalue',
                           b'cf1:c5': b'anothervalue'})
            b.delete(b'row', [b'cf1:c3'])

        with self.assertRaises(ValueError):
            with self.table.batch(transaction=True) as b:
                b.put(b'fooz', {b'cf1:bar': b'baz'})
                raise ValueError
        self.assertDictEqual({}, self.table.row(b'fooz', [b'cf1:bar']))

        with self.assertRaises(ValueError):
            with self.table.batch(transaction=False) as b:
                b.put(b'fooz', {b'cf1:bar': b'baz'})
                raise ValueError
        self.assertDictEqual(
            {b'cf1:bar': b'baz'},
            self.table.row(b'fooz', [b'cf1:bar']),
        )

        with self.table.batch(batch_size=5) as b:
            for i in range(10):
                b.put(
                    f'row-batch1-{i:03}'.encode('ascii'),
                    {b'cf1:': str(i).encode('ascii')},
                )

        with self.table.batch(batch_size=20) as b:
            for i in range(95):
                b.put(
                    f'row-batch2-{i:03}'.encode('ascii'),
                    {b'cf1:': str(i).encode('ascii')},
                )
        self.assertEqual(95, self._scan_len(row_prefix=b'row-batch2-'))

        with self.table.batch(batch_size=20) as b:
            for i in range(95):
                b.delete(f'row-batch2-{i:03}'.encode('ascii'))
        self.assertEqual(0, self._scan_len(row_prefix=b'row-batch2-'))

    def test_row(self):
        row = self.table.row
        put = self.table.put
        row_key = b'row-test'

        with self.assertRaises(TypeError):
            row(row_key, 123)  # noqa

        with self.assertRaises(TypeError):
            row(row_key, timestamp='invalid')  # noqa

        put(row_key, {b'cf1:col1': b'v1old'}, timestamp=1234)
        put(row_key, {b'cf1:col1': b'v1new'}, timestamp=3456)
        put(row_key, {b'cf1:col2': b'v2', b'cf2:col1': b'v3'})
        put(row_key, {b'cf2:col2': b'v4'}, timestamp=1234)

        exp = {b'cf1:col1': b'v1new',
               b'cf1:col2': b'v2',
               b'cf2:col1': b'v3',
               b'cf2:col2': b'v4'}
        self.assertDictEqual(exp, row(row_key))

        exp = {b'cf1:col1': b'v1new', b'cf1:col2': b'v2'}
        self.assertDictEqual(exp, row(row_key, [b'cf1']))

        exp = {b'cf1:col1': b'v1new', b'cf2:col2': b'v4'}
        self.assertDictEqual(exp, row(row_key, list(exp)))

        exp = {b'cf1:col1': b'v1old', b'cf2:col2': b'v4'}
        self.assertDictEqual(exp, row(row_key, timestamp=2345))

        self.assertDictEqual({}, row(row_key, timestamp=123))

        res = row(row_key, include_timestamp=True)
        self.assertEqual(len(res), 4)
        self.assertEqual(b'v1new', res[b'cf1:col1'][0])
        self.assertIsInstance(res[b'cf1:col1'][1], int)

    def test_rows(self):
        row_keys = [b'rows-row1', b'rows-row2', b'rows-row3']
        data_old = {b'cf1:col1': b'v1old', b'cf1:col2': b'v2old'}
        data_new = {b'cf1:col1': b'v1new', b'cf1:col2': b'v2new'}

        with self.assertRaises(TypeError):
            self.table.rows(row_keys, object())  # noqa

        with self.assertRaises(TypeError):
            self.table.rows(row_keys, timestamp='invalid')  # noqa

        for row_key in row_keys:
            self.table.put(row_key, data_old, timestamp=4000)

        for row_key in row_keys:
            self.table.put(row_key, data_new)

        self.assertDictEqual({}, dict(self.table.rows([])))

        rows = dict(self.table.rows(row_keys))
        for row_key in row_keys:
            self.assertIn(row_key, rows)
            self.assertDictEqual(data_new, rows[row_key])

        rows = dict(self.table.rows(row_keys, timestamp=5000))
        for row_key in row_keys:
            self.assertIn(row_key, rows)
            self.assertDictEqual(data_old, rows[row_key])

    def test_cells(self):
        row_key = b'cell-test'
        col = b'cf1:col1'

        self.table.put(row_key, {col: b'old'}, timestamp=1234)
        self.table.put(row_key, {col: b'new'})

        with self.assertRaises(TypeError):
            self.table.cells(row_key, col, versions='invalid')  # noqa

        with self.assertRaises(TypeError):
            self.table.cells(
                row_key, col,
                versions=3,
                timestamp='invalid',  # noqa
            )

        with self.assertRaises(ValueError):
            self.table.cells(row_key, col, versions=0)

        results = self.table.cells(row_key, col, versions=1)
        self.assertEqual(len(results), 1)
        self.assertEqual(b'new', results[0])

        results = self.table.cells(row_key, col)
        self.assertEqual(len(results), 2)
        self.assertEqual(b'new', results[0])
        self.assertEqual(b'old', results[1])

        results = self.table.cells(
            row_key, col,
            timestamp=2345,
            include_timestamp=True,
        )
        self.assertEqual(len(results), 1)
        self.assertEqual(b'old', results[0][0])
        self.assertEqual(1234, results[0][1])

    def test_scan(self):
        with self.assertRaises(TypeError):
            self._scan_list(row_prefix='foobar', row_start='xyz')

        if self.connection.compat == '0.90':
            with self.assertRaises(NotImplementedError):
                self._scan_list(filter='foo')

        with self.assertRaises(ValueError):
            self._scan_list(limit=0)

        with self.table.batch() as b:
            for i in range(2000):
                b.put(f'row-scan-a{i:05}'.encode('ascii'),
                      {b'cf1:col1': b'v1',
                       b'cf1:col2': b'v2',
                       b'cf2:col1': b'v1',
                       b'cf2:col2': b'v2'})
                b.put(f'row-scan-b{i:05}'.encode('ascii'),
                      {b'cf1:col1': b'v1', b'cf1:col2': b'v2'})

        scanner = self.table.scan(
            row_start=b'row-scan-a00012',
            row_stop=b'row-scan-a00022',
        )
        self.assertEqual(10, self._scan_len(scanner))

        scanner = self.table.scan(row_start=b'xyz')
        self.assertEqual(0, self._scan_len(scanner))

        scanner = self.table.scan(row_start=b'xyz', row_stop=b'zyx')
        self.assertEqual(0, self._scan_len(scanner))

        rows = self._scan_list(
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
        self.assertEqual(1000, self._scan_len(scanner))

        scanner = self.table.scan(
            row_prefix=b'row-scan-b',
            batch_size=1,
            limit=10,
        )
        self.assertEqual(10, self._scan_len(scanner))

        scanner = self.table.scan(
            row_prefix=b'row-scan-b',
            batch_size=5,
            limit=10,
        )
        self.assertEqual(10, self._scan_len(scanner))

        scanner = self.table.scan(timestamp=123)
        self.assertEqual(0, self._scan_len(scanner))

        scanner = self.table.scan(row_prefix=b'row', timestamp=123)
        self.assertEqual(0, self._scan_len(scanner))

        scanner = self.table.scan(batch_size=20)
        next(scanner)
        scanner.close()
        with self.assertRaises(StopIteration):
            next(scanner)

    def test_scan_sorting(self):
        if self.connection.compat < '0.96':
            return  # not supported

        input_row = {f'cf1:col-{i:03}'.encode('ascii'): b'' for i in range(100)}
        input_key = b'row-scan-sorted'
        self.table.put(input_key, input_row)

        scan = self.table.scan(row_start=input_key, sorted_columns=True)
        key, row = next(scan)
        self.assertEqual(key, input_key)
        self.assertListEqual(sorted(input_row.items()), list(row.items()))
        scan.close()

    def test_scan_reverse(self):

        if self.connection.compat < '0.98':
            with self.assertRaises(NotImplementedError):
                self._scan_list(reverse=True)
            return

        with self.table.batch() as b:
            for i in range(2000):
                b.put(f'row-scan-reverse-{i:04}'.encode('ascii'),
                      {b'cf1:col1': b'v1', b'cf1:col2': b'v2'})

        scan = self.table.scan(row_prefix=b'row-scan-reverse', reverse=True)
        self.assertEqual(2000, self._scan_len(scan))

        self.assertEqual(10, self._scan_len(limit=10, reverse=True))

        scan = self.table.scan(
            row_start=b'row-scan-reverse-1999',
            row_stop=b'row-scan-reverse-0000',
            reverse=True,
        )
        key, data = next(scan)
        self.assertEqual(b'row-scan-reverse-1999', key)

        key, data = [x for x in scan][-1]
        self.assertEqual(b'row-scan-reverse-0001', key)

    def test_scan_filter_and_batch_size(self):
        # See issue #54 and #56
        filt = b"SingleColumnValueFilter('cf1','col1',=,'binary:%s',true,true)"

        for _ in self.table.scan(filter=filt % b'hello there'):
            self.fail("There is no cf1:col1='hello there'")

        got_results = False
        for k, v in self.table.scan(filter=filt % b'v1'):
            got_results = True
            self.assertIsNotNone(next((x for x in v if b'cf1' in x), None))
            self.assertEqual(v[b'cf1:col1'], b'v1')

        if not got_results:
            self.fail("No results found for cf1:col1='v1'")

    def test_delete(self):
        row_key = b'row-test-delete'
        data = {b'cf1:col1': b'v1',
                b'cf1:col2': b'v2',
                b'cf1:col3': b'v3'}
        self.table.put(row_key, {b'cf1:col2': b'v2old'}, timestamp=1234)
        self.table.put(row_key, data)

        self.table.delete(row_key, [b'cf1:col2'], timestamp=2345)
        cells = self.table.cells(row_key, b'cf1:col2', versions=2)
        self.assertEqual(1, len(cells))
        self.assertDictEqual(data, self.table.row(row_key))

        self.table.delete(row_key, [b'cf1:col1'])
        res = self.table.row(row_key)
        self.assertNotIn(b'cf1:col1', res)
        self.assertIn(b'cf1:col2', res)
        self.assertIn(b'cf1:col3', res)

        self.table.delete(row_key, timestamp=12345)
        res = self.table.row(row_key)
        self.assertIn(b'cf1:col2', res)
        self.assertIn(b'cf1:col3', res)

        self.table.delete(row_key)
        self.assertDictEqual({}, self.table.row(row_key))

    def test_connection_pool_construction(self):
        with self.assertRaises(TypeError):
            ConnectionPool(size='abc', autoconnect=False)  # noqa

        with self.assertRaises(ValueError):
            ConnectionPool(size=0, autoconnect=False)

    def test_connection_pool(self):

        def run():
            thread_name = current_thread().name
            print(f"Thread {thread_name} starting")

            def inner_function():
                # Nested connection requests must return the same connection
                with pool.connection() as another_connection:
                    self.assertIs(connection, another_connection)

                    # Fake an exception once in a while
                    if random.random() < .25:
                        print("Introducing random failure")
                        connection.transport.close()
                        raise TException("Fake transport exception")

            for i in range(50):
                with pool.connection() as connection:
                    connection.tables()

                    try:
                        inner_function()
                    except TException:
                        # This error should have been picked up by the
                        # connection pool, and the connection should have
                        # been replaced by a fresh one
                        pass

                    connection.tables()

            print(f"Thread {thread_name} done")

        with ConnectionPool(size=3, **connection_kwargs) as pool:
            threads = [Thread(target=run) for _ in range(10)]
            for t in threads:
                t.start()

            while threads:
                for t in threads:
                    t.join(timeout=.1)

                # filter out finished threads
                threads = [t for t in threads if t.is_alive()]
                print(f"{len(threads)} threads still alive")

    def test_pool_exhaustion(self):

        def run():
            with self.assertRaises(NoConnectionsAvailable):
                with pool.connection(timeout=.1) as connection:
                    connection.tables()

        with ConnectionPool(size=1, **connection_kwargs) as pool:
            with pool.connection():
                # At this point the only connection is assigned to this task,
                # so another task cannot obtain a connection.
                t = Thread(target=run)
                t.start()
                t.join()


if __name__ == '__main__':
    import logging
    import sys

    # Dump stacktraces using 'kill -USR1', useful for debugging hanging
    # programs and multi threading issues.
    try:
        import faulthandler  # noqa
    except ImportError:
        pass
    else:
        import signal
        faulthandler.register(signal.SIGUSR1)

    logging.basicConfig(level=logging.DEBUG)

    method_name = f'test_{sys.argv[1]}'
    method = globals()[method_name]
    method()
