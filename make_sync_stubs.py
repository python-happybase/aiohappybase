"""
Simple script to generate typing stub files for the public classes in
aiohappybase.sync to enable IDE type checking and auto-completion.
Automatically run when generating PyPI releases.
"""
import re
from textwrap import dedent
from unittest import mock
from pathlib import Path
from inspect import getsource

import autopep8  # noqa

# Ignore any unknown imports while loading classes
mock.patch.dict('sys.modules', unknown=mock.MagicMock())

import aiohappybase  # noqa
from aiohappybase.sync._util import _make_sub  # noqa

SUBSTITUTIONS = [
    _make_sub(r'(?<!\w)(async|await)\s?'),  # Remove async/await
    _make_sub(r'""".*?"""', flags=re.DOTALL),  # Remove doc-strings
    _make_sub(r'#.*$', flags=re.MULTILINE),  # Remove comments
    _make_sub(r'(\n\s*)+\n', repl='\n\n'),  # Remove blanklines
    _make_sub(  # Remove function body
        r'(def .*?:$).*?(?=(^\s*((def )|(@\w+)))|\Z)',
        repl=r'\1\n        ...\n',
        flags=re.DOTALL | re.MULTILINE,
    ),
    _make_sub(  # Remove async meta methods
        r'(def __a\w+__.*?:$).*?(?=(^\s*def )|\Z)',
        repl='',
        flags=re.DOTALL | re.MULTILINE,
    ),
    _make_sub('(?<=T)Async'),  # Remove Async from thrift imports
    _make_sub(  # Convert typing AsyncGenerator to Generator
        r'AsyncGen\w*\[(.*)\]',
        repl=r'Generator[\1, None]',
    ),
    _make_sub(  # Unwrap Awaitable typing
        r'Awaitable\[(.*)\]',
        repl=r'\1',
    ),
]

if __name__ == '__main__':
    for name in ['Connection', 'Batch', 'ConnectionPool', 'Table']:
        cls = getattr(aiohappybase, name)
        mod = cls.__module__.split('.')[-1]

        imports = dedent(f"""\
            from contextlib import *
            from typing import *
            
            from thriftpy2.transport import *
            from thriftpy2.protocol import *
            from thriftpy2.thrift import *
            
            from ..{mod} import *
            from . import *
        """)

        code = getsource(cls)
        for sub in SUBSTITUTIONS:
            code = sub(code)

        code = autopep8.fix_code(imports + code, options={'aggressive': 2})

        with Path('aiohappybase', 'sync', f'{mod}.pyi').open('w') as f:
            f.write(code)
