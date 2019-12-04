"""
HappyBase tests.
"""

import os
import unittest
from functools import partial
from threading import Thread, current_thread

from aiohappybase.sync import *  # noqa - For synchronize()
from aiohappybase.sync._util import synchronize  # noqa

from tests.test_api import TestAPI as AsyncTestAPI

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


@partial(synchronize, base=AsyncTestAPI)
class TestSyncAPI(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.create_table()

    @classmethod
    def tearDownClass(cls):
        cls.destroy_table()

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
