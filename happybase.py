"""Compatibility module with the original happybase."""

__all__ = [
    'DEFAULT_HOST',
    'DEFAULT_PORT',
    'Connection',
    'Table',
    'Batch',
    'ConnectionPool',
    'NoConnectionsAvailable',
]

from aiohappybase.sync import *
from aiohappybase.sync import __version__  # noqa
