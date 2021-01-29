"""
HappyBase tests.
"""

from threading import Thread, current_thread

import pytest

from aiohappybase.sync import *  # noqa - For synchronize()
from aiohappybase.sync._util import synchronize  # noqa

from tests.test_api import TestAPI as AsyncTestAPI, connection_kwargs


@pytest.fixture
def conn() -> Connection:
    with Connection(**connection_kwargs) as conn:
        assert conn is not None
        yield conn


@pytest.fixture
def table(conn: Connection, table_name: bytes) -> Table:
    cfs = {
        'cf1': {},
        'cf2': None,
        'cf3': {'max_versions': 1},
    }
    table = conn.create_table(table_name, families=cfs)
    assert table is not None
    yield table
    conn.delete_table(table_name, disable=True)


@synchronize(base=AsyncTestAPI)
class TestSyncAPI:

    @staticmethod
    def _run_tasks(func, count: int = 1):
        threads = [Thread(target=func) for _ in range(count)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

    @staticmethod
    def _current_task_name() -> str:
        return f"Thread {current_thread().name}"


del AsyncTestAPI  # Don't run these tests here


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
