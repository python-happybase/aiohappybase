import re
from functools import partial
from importlib import import_module
from textwrap import dedent
from typing import Callable, TypeVar, Awaitable, Dict, Any, Tuple, Union
from contextlib import contextmanager
from types import FunctionType, CodeType
from inspect import (
    signature,
    getsourcefile,
    getsourcelines,
    iscoroutinefunction,
    isasyncgenfunction,
)

T = TypeVar('T')


def _make_sub(pat: str, repl: str = '', flags: int = 0) -> Callable[[str], str]:
    """Create a regex substitution function for the given arguments."""
    return partial(re.compile(pat, flags=flags).sub, repl)  # noqa


_remove_async = _make_sub(r'(?<!\w)(async |await |async(?=contextmanager))')
_convert_async_names = _make_sub(
    # Match __aenter__, __aexit__, __anext__, aclose, StopAsyncIteration
    r'(?:(__)a(enter|exit|next)(__))|a(close)|(Stop)Async(Iteration)',
    # Replace with __enter__, __exit__, __next__, close, StopIteration
    repl=r'\1\2\3\4\5\6',
)


def synchronize(cls: type = None, base: type = None) -> type:
    """
    Class decorator for classes in the sync package to copy any undefined
    functionality from their async equivalents. All async functions will
    automatically be converted to sync versions by stripping the async/await.
    The compiler will declare that they occurred at the same line as the
    original async one to allow debugging, however the await part of any
    statements will have no effect (as it doesn't exist anymore).
    """
    # Support base=X in decorator form
    if cls is None:
        return partial(synchronize, base=base)
    if base is None:
        # Get the equivalent async class if none given
        base = getattr(
            import_module(cls.__module__.replace('.sync', '')),
            cls.__name__,
        )
    # Define the global scope for creating functions to ensure any
    # look-ups find the right values.
    scope = {
        'contextmanager': contextmanager,  # Convert asynccontextmanager
        **import_module(base.__module__).__dict__,  # Scope of base class module
        **base.__dict__,  # Scope of base class
        **import_module('aiohappybase.sync').__dict__,  # Scope of sync pkg
        **import_module(cls.__module__).__dict__,  # Scope of class module
        **cls.__dict__,  # Scope inside the class
        cls.__name__: cls,
    }
    # Copy all undefined attributes
    for name, value in base.__dict__.items():
        if name in cls.__dict__:
            continue
        unwrapped = _unwrap(value)
        if isinstance(unwrapped, FunctionType):
            value = _synchronize_func(unwrapped, scope)
            # Name could change like __aenter__ -> __enter__
            # Get the new one while bypassing decorators
            name = _unwrap(value).__name__
        setattr(cls, name, value)
    # Ensure doc-string is copied if necessary
    cls.__doc__ = cls.__doc__ or base.__doc__
    return cls


def _synchronize_func(func: Callable[..., Union[T, Awaitable[T]]],
                      scope: Dict[str, Any]) -> Callable[..., T]:
    """
    Re-compile the given function without async/await keywords to ensure
    that it is synchronous while retaining the file and line number meta
    data so that debugging is still possible.

    :param func: Function to ensure is synchronous
    :param scope: Scope to exec the new function in
    :return: Converted sync function
    """
    lnum, code = _get_source(func)
    if _is_async_func(func):
        # Convert standard async names to sync equivalent
        code = _convert_async_names(code)
        # Remove async/await everywhere
        code = _remove_async(code)
    # Find instances of super() and replace with super(ClassName, self)
    if 'super()' in code:
        class_name = func.__qualname__.split('.')[0]
        self_name = next(iter(signature(func).parameters))
        code = code.replace('super()', f'super({class_name}, {self_name})')
    # Compile code at the same line number as the async code to allow debugging
    code = compile('\n' * (lnum - 1) + code, getsourcefile(func), 'exec')
    # Execute the code which will add the function to the local dict
    return _exec_code(code, scope)


def _exec_code(code: CodeType, scope: Dict[str, Any]) -> Callable[..., T]:
    """
    Execute a given code object in a new scope and return the result.

    :param code: Code object to execute
    :param scope: Scope to exec the new function in
    :return: New function
    """
    local = {}
    exec(code, scope, local)
    return next(iter(local.values()))  # Retrieve the function and return it


def _get_source(func: FunctionType) -> Tuple[int, str]:
    """Get the line number and source code for a given function."""
    # Get the text for the function
    lines, line_number = getsourcelines(func)
    code = dedent(''.join([*(), *lines]))
    return line_number, code


def _is_async_func(value: Any) -> bool:
    """Determine if a given value is a function defined with async."""
    return iscoroutinefunction(value) or isasyncgenfunction(value)


def _unwrap(value: Any) -> Any:
    """Like inspect.unwrap but also handles static/class methods."""
    for attr in ('__wrapped__', '__func__', 'fget'):
        try:
            return _unwrap(getattr(value, attr))
        except AttributeError:
            pass
    return value
