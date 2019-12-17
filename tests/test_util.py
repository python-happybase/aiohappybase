"""
HappyBase utility tests.
"""

import unittest as ut
from inspect import iscoroutinefunction
from codecs import decode, encode

from aiohappybase import _util as util  # noqa
from aiohappybase.sync import _util as sync_util  # noqa
from aiohappybase.pool import asynccontextmanager


class TestUtil(ut.TestCase):
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
            self.assertEqual(correct, x1)
            self.assertEqual(correct, x2)

            y1 = util.snake_to_camel_case(x1, True)
            y2 = util.snake_to_camel_case(x2, False)
            self.assertEqual(upper_cc, y1)
            self.assertEqual(lower_cc, y2)

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

        self.assertIsNone(util.bytes_increment(b'\xff\xff\xff'))

        for s, expected in test_values:
            s = decode(s, 'hex')
            v = util.bytes_increment(s)
            v_hex = encode(v, 'hex')
            self.assertEqual(expected, v_hex)
            self.assertLess(s, v)

    def test_ensure_bytes(self):
        self.assertIsInstance(util.ensure_bytes('str'), bytes)
        self.assertEqual(util.ensure_bytes('str'), b'str')

        self.assertIsInstance(util.ensure_bytes(b'bytes'), bytes)
        self.assertEqual(util.ensure_bytes(b'bytes'), b'bytes')

        for x in [1, ('test',), float('inf')]:
            with self.assertRaises(TypeError):
                util.ensure_bytes(x)

    def test_remove_async(self):
        r = sync_util._remove_async
        eq = self.assertEqual
        eq(
            r("""
            async def x():
                await some_func()
                yield y(await other_coro)
            """),
            """
            def x():
                some_func()
                yield y(other_coro)
            """
        )

    def test_convert_async_std_methods(self):
        c = sync_util._convert_async_names
        eq = self.assertEqual

        eq(c('__aenter__'), '__enter__')
        eq(c('__aexit__'), '__exit__')
        eq(c('__anext__'), '__next__')
        eq(c('aclose'), 'close')
        eq(c('StopAsyncIteration'), 'StopIteration')

        eq(c('aenter'), 'aenter')
        eq(c('231!452ca\t123cap__aenter__\n'), '231!452ca\t123cap__enter__\n')

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

        self.assertTrue(iscoroutinefunction(A.x))
        self.assertTrue(iscoroutinefunction(A.y))
        self.assertTrue(iscoroutinefunction(A.__aenter__))

        self.assertTrue(hasattr(B, 'x'))
        self.assertFalse(iscoroutinefunction(B.x))

        self.assertTrue(hasattr(B, 'y'))
        self.assertFalse(iscoroutinefunction(B.y))

        self.assertFalse(hasattr(B, '__aenter__'))
        self.assertTrue(hasattr(B, '__enter__'))
        self.assertFalse(iscoroutinefunction(B.__enter__))

        self.assertFalse(hasattr(B, '__aexit__'))
        self.assertTrue(hasattr(B, '__exit__'))
        self.assertFalse(iscoroutinefunction(B.__exit__))

        self.assertEqual(B().x(), 10)
        self.assertEqual(B().y(), 10)

        with B() as b:
            self.assertIsInstance(b, B)

        with B().context() as b:
            self.assertIsInstance(b, B)
