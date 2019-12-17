"""
HappyBase utility module.

These functions are not part of the public API.
"""
import re
from functools import lru_cache
from typing import (
    Dict,
    List,
    Tuple,
    Any,
    AnyStr,
    Optional,
    TypeVar,
    Callable,
    Iterable,
)

from Hbase_thrift import TRowResult, TCell

T = TypeVar('T')

KTI = TypeVar('KTI')
VTI = TypeVar('VTI')

KTO = TypeVar('KTO')
VTO = TypeVar('VTO')

CAPITALS = re.compile('([A-Z])')


@lru_cache(maxsize=None)
def camel_to_snake_case(name: str) -> str:
    """Convert a CamelCased name to PEP8 style snake_case."""
    return CAPITALS.sub(r'_\1', name).lower().lstrip('_')


@lru_cache(maxsize=None)
def snake_to_camel_case(name: str, initial: bool = False) -> str:
    """Convert a PEP8 style snake_case name to CamelCase."""
    chunks = name.split('_')
    converted = [s.capitalize() for s in chunks]
    if initial:
        return ''.join(converted)
    else:
        return chunks[0].lower() + ''.join(converted[1:])


def thrift_attrs(obj_or_cls) -> List[str]:
    """Obtain Thrift data type attribute names for an instance or class."""
    return [v[1] for v in obj_or_cls.thrift_spec.values()]


def thrift_type_to_dict(obj: Any) -> Dict[str, Any]:
    """Convert a Thrift data type to a regular dictionary."""
    return {
        camel_to_snake_case(attr): getattr(obj, attr)
        for attr in thrift_attrs(obj)
    }


def ensure_bytes(value: AnyStr) -> bytes:
    """Convert text into bytes, and leaves bytes as-is."""
    if isinstance(value, bytes):
        return value
    if isinstance(value, str):
        return value.encode('utf-8')
    raise TypeError(
        f"input must be a text or byte string, got {type(value).__name__}"
    )


def bytes_increment(b: bytes) -> Optional[bytes]:
    """
    Increment and truncate a byte string (for sorting purposes)

    This functions returns the shortest string that sorts after the given
    string when compared using regular string comparison semantics.

    This function increments the last byte that is smaller than ``0xFF``, and
    drops everything after it. If the string only contains ``0xFF`` bytes,
    `None` is returned.
    """
    assert isinstance(b, bytes)
    b = bytearray(b)  # Used subset of its API is the same on Python 2 and 3.
    for i in range(len(b) - 1, -1, -1):
        if b[i] != 0xff:
            b[i] += 1
            return bytes(b[:i+1])
    return None


def _id(x: T) -> T: return x


def map_dict(data: Dict[KTI, VTI],
             keys: Callable[[KTI], KTO] = _id,
             values: Callable[[VTI], VTO] = _id) -> Dict[KTO, VTO]:
    """
    Dictionary mapping function, analogous to :py:func:`builtins.map`. Allows
    applying a specific function independently to both the keys and values.

    :param data: Dictionary to apply mapping to
    :param keys: Optional function to apply to all keys
    :param values: Optional function to apply to all values
    :return: New dictionary with keys and values mapped
    """
    return {keys(k): values(v) for k, v in data.items()}


def make_row(row: TRowResult) -> Dict[bytes, bytes]:
    """
    Make a row dict for a given row result.

    :param row: Row result from thrift client to convert a row dictionary
    :return: Dictionary mapping columns to values for the row.
    """
    return {name: cell.value for name, cell in _get_cell_map(row).items()}


def make_row_ts(row: TRowResult) -> Dict[bytes, Tuple[bytes, int]]:
    """
    Make a row dict for a given row result including timestamps.

    :param row: Row result from thrift client to convert a row dictionary
    :return: Dictionary mapping columns to tuples of (value, timestamp)
    """
    return {
        name: (cell.value, cell.timestamp)
        for name, cell in _get_cell_map(row).items()
    }


def _get_cell_map(row: TRowResult) -> Dict[bytes, TCell]:
    """Convert a row result to dictionary mapping column names to cells."""
    if row.sortedColumns is not None:
        return {c.columnName: c.cell for c in row.sortedColumns}
    elif row.columns is not None:
        return row.columns
    else:  # pragma: no cover
        raise RuntimeError("Neither columns nor sortedColumns is available!")


def check_invalid_items(**kwargs: Tuple[T, Iterable[T]]):
    """
    Check if a parameter's value is within a valid set of values. Multiple
    parameters can be checked at once.

    :param kwargs:
        Parameter names mapped to tuples of actual value and possible values.
    :raises ValueError: If a parameter value is not valid
    """
    for key, (value, possible) in kwargs.items():
        possible = set(possible)
        if value not in possible:
            raise ValueError(f"{key}={value} is not in: {possible}")
