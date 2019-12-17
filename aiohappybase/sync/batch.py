"""
HappyBase Batch module.
"""

import logging
import abc

from ._util import synchronize

logger = logging.getLogger(__name__)


@synchronize
class Batcher(abc.ABC):
    ...


@synchronize
class MutationBatcher(Batcher):
    ...


@synchronize
class CounterBatcher(Batcher):
    ...


@synchronize
class Batch:
    ...
