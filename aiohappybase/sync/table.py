"""
HappyBase table module.
"""

import logging

from ._util import synchronize
from .batch import Batch

logger = logging.getLogger(__name__)


@synchronize
class Table:
    BATCH_TYPE = Batch
