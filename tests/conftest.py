import secrets

import pytest


@pytest.fixture
def table_name() -> bytes:
    return b'test_' + secrets.token_hex(5).encode()
