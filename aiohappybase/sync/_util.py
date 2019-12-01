import re
from functools import partial
from importlib import import_module
from textwrap import dedent
from typing import Callable, TypeVar, Awaitable, Dict, Any, Tuple
from contextlib import contextmanager
from inspect import (
    getsourcefile,
    getsourcelines,
    iscoroutinefunction,
    isasyncgenfunction,
    unwrap,
)

import aiohappybase

T = TypeVar('T')


def _make_sub(pat: str, repl: str = '', flags: int = 0) -> Callable[[str], str]:
    """Create a regex substitution function for the given arguments."""
    return partial(re.compile(pat, flags=flags).sub, repl)  # noqa


_remove_async = _make_sub(r'(?<!\w)(async|await)\s?')
_convert_async_meta_methods = _make_sub(r'__a(\w+)__', repl=r'__\1__')


def synchronize(cls: type) -> type:
    """
    Class decorator for classes in the sync package to copy any undefined
    functionality from their async equivalents. All async functions will
    automatically be converted to sync versions by stripping the async/await.
    The compiler will declare that they occurred at the same line as the
    original async one to allow debugging, however the await part of any
    statements will have no effect (as it doesn't exist anymore).
    """
    # Get the equivalent async class
    base = getattr(aiohappybase, cls.__name__)
    # Define the global scope for creating functions to ensure any
    # look-ups find the right values.
    scope = {
        'contextmanager': contextmanager,  # Convert asynccontextmanager
        **import_module(base.__module__).__dict__,  # Scope of base class module
        **base.__dict__,  # Scope of base class
        **import_module(cls.__module__).__dict__,  # Scope of class module
        **cls.__dict__,  # Scope inside the class
    }
    # Copy all undefined attributes
    for name, value in base.__dict__.items():
        if name in cls.__dict__:
            continue
        if _is_async_func(value):
            value = _synchronize_func(value, scope)
            name = value.__name__  # Could change: __aenter__ -> __enter__
        setattr(cls, name, value)
    return cls


def _synchronize_func(func: Callable[..., Awaitable[T]],
                      scope: Dict[str, Any]) -> Tuple[str, Callable[..., T]]:
    """
    Convert a given function from async to sync while retaining the file and
    line number meta data so that debugging is still possible.

    :param func: Async function to convert
    :param scope: Scope to exec the new function in
    :return: Converted sync function
    """
    # Get the text for the function
    lines, line_number = getsourcelines(func)
    code = dedent(''.join([*(), *lines]))
    # Convert async meta methods to sync equivalent (__aenter__ -> __enter__)
    code = _convert_async_meta_methods(code)
    # Remove async/await everywhere
    code = _remove_async(code)
    # Compile code at the same line number as the async code to allow debugging
    code = compile('\n' * (line_number - 1) + code, getsourcefile(func), 'exec')
    # Execute the code which will add the function to the local dict
    local = {}
    exec(code, scope, local)
    return next(iter(local.values()))  # Retrieve the function and return it


def _is_async_func(value: Any) -> bool:
    """Determine if a given value is a function defined with async."""
    value = unwrap(value)  # Remove wrappers, like asynccontextmanager
    return iscoroutinefunction(value) or isasyncgenfunction(value)
