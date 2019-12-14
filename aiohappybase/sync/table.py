"""
HappyBase table module.
"""

import logging

from ._util import synchronize

logger = logging.getLogger(__name__)


@synchronize
class Table:
    ...
