"""
HappyBase utility tests.
"""

import pytest

from inspect import iscoroutinefunction
from codecs import decode, encode

from aiohappybase import _util as util  # noqa
from aiohappybase.sync import _util as sync_util  # noqa
from aiohappybase.pool import asynccontextmanager


class TestUtil:
    def test_camel_to_snake_case(self):
        examples = [
            ('foo', 'Foo', 'foo'),
            ('fooBar', 'FooBar', 'foo_bar'),
            ('fooBarBaz', 'FooBarBaz', 'foo_bar_baz'),
            ('fOO', 'FOO', 'f_o_o'),
        ]

        for lower_cc, upper_cc, correct in examples:
            x1 = util.camel_to_snake_case(lower_cc)
            x2 = util.camel_to_snake_case(upper_cc)
            assert correct == x1
            assert correct == x2

            y1 = util.snake_to_camel_case(x1, True)
            y2 = util.snake_to_camel_case(x2, False)
            assert upper_cc == y1
            assert lower_cc == y2

    def test_bytes_increment(self):
        test_values = [
            (b'00', b'01'),
            (b'01', b'02'),
            (b'fe', b'ff'),
            (b'1234', b'1235'),
            (b'12fe', b'12ff'),
            (b'12ff', b'13'),
            (b'424242ff', b'424243'),
            (b'4242ffff', b'4243'),
        ]

        assert util.bytes_increment(b'\xff\xff\xff') is None

        for s, expected in test_values:
            s = decode(s, 'hex')
            v = util.bytes_increment(s)
            v_hex = encode(v, 'hex')
            assert expected == v_hex
            assert s < v

    def test_ensure_bytes(self):
        assert isinstance(util.ensure_bytes('str'), bytes)
        assert util.ensure_bytes('str') == b'str'

        assert isinstance(util.ensure_bytes(b'bytes'), bytes)
        assert util.ensure_bytes(b'bytes') == b'bytes'

        for x in [1, ('test',), float('inf')]:
            with pytest.raises(TypeError):
                util.ensure_bytes(x)

    def test_remove_async(self):
        r = sync_util._remove_async
        assert (
            r("""
            async def x():
                await some_func()
                yield y(await other_coro)
            """) ==
            """
            def x():
                some_func()
                yield y(other_coro)
            """
        )

    def test_convert_async_std_methods(self):
        c = sync_util._convert_async_names

        assert c('__aenter__') == '__enter__'
        assert c('__aexit__') == '__exit__'
        assert c('__anext__') == '__next__'
        assert c('aclose') == 'close'
        assert c('StopAsyncIteration') == 'StopIteration'

        assert c('aenter') == 'aenter'
        assert (c('231!452ca\t123cap__aenter__\n')
                == '231!452ca\t123cap__enter__\n')

    def test_synchronize(self):
        class A:
            async def x(self):
                return 10

            async def y(self):
                return await self.x()

            async def __aenter__(self):
                return self

            async def __aexit__(self, *_):
                pass

            @asynccontextmanager
            async def context(self):
                yield self

        @sync_util.synchronize(base=A)
        class B:
            pass

        assert iscoroutinefunction(A.x)
        assert iscoroutinefunction(A.y)
        assert iscoroutinefunction(A.__aenter__)

        assert hasattr(B, 'x')
        assert not iscoroutinefunction(B.x)

        assert hasattr(B, 'y')
        assert not iscoroutinefunction(B.y)

        assert not hasattr(B, '__aenter__')
        assert hasattr(B, '__enter__')
        assert not iscoroutinefunction(B.__enter__)

        assert not hasattr(B, '__aexit__')
        assert hasattr(B, '__exit__')
        assert not iscoroutinefunction(B.__exit__)

        assert B().x() == 10
        assert B().y() == 10

        with B() as b:
            assert isinstance(b, B)

        with B().context() as b:
            assert isinstance(b, B)
