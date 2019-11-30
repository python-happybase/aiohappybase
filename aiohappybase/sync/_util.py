import re
from functools import partial
from importlib import import_module
from textwrap import dedent
from typing import Callable, TypeVar, Awaitable, Dict, Any, Tuple
from contextlib import contextmanager
from inspect import (
    getsource,
    iscoroutinefunction,
    isasyncgenfunction,
    unwrap,
)

import aiohappybase

T = TypeVar('T')

_async_pat = re.compile(r'((?<!\w)(async|await) )|async(?=contextmanager)')
_remove_async = partial(_async_pat.sub, '')
_async_meta_method = re.compile(r'__a(\w+)__')


def synchronize(cls: type) -> type:
    base = getattr(aiohappybase, cls.__name__)
    scope = {
        'contextmanager': contextmanager,
        **import_module(base.__module__).__dict__,
        **base.__dict__,
        **import_module(cls.__module__).__dict__,
        **cls.__dict__,
    }
    for name, value in base.__dict__.items():
        if name in cls.__dict__:
            continue
        if is_async_func(value):
            name, value = synchronize_func(name, value, scope)
        setattr(cls, name, value)
    return cls


def is_async_func(value: Any) -> bool:
    value = unwrap(value)
    return iscoroutinefunction(value) or isasyncgenfunction(value)


def synchronize_func(name: str,
                     func: Callable[..., Awaitable[T]],
                     scope: Dict[str, Any]) -> Tuple[str, Callable[..., T]]:
    code = dedent(getsource(func))
    match = _async_meta_method.match(name)
    if match:  # __aenter__, __aexit__, etc.
        new_name = f'__{match.groups()[0]}__'
        code = re.sub(name, new_name, code)
        name = new_name
    code = _remove_async(code)
    local = {}
    exec(code, scope, local)
    return name, local[name]
