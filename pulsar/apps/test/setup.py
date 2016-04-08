import sys
import shlex
from importlib import import_module

import setuptools.command.test as orig


skip_settings = ['version', 'daemon', 'reload', 'process_name',
                 'user', 'group', 'max_requests',
                 'password', 'timeout', 'exc-id']


TestSuite = import_module('pulsar.apps.test').TestSuite


def get_test_options():
    yield 'labels=', None, 'Labels to include in tests'
    for setting in TestSuite.cfg.settings.values():
        if setting.name in skip_settings or not setting.flags:
            continue
        long = None
        short = None
        for flag in setting.flags:
            if flag.startswith('--'):
                long = flag[2:]
                if setting.default is not False:
                    long = '%s=' % long
                    short = None
                    break
            else:
                short = flag[1:]

        yield long, short, setting.desc


class Test(orig.test):
    test_suite = TestSuite
    user_options = list(get_test_options())

    def initialize_options(self):
        for name, value in options(self):
            setattr(self, name, value)

    def finalize_options(self):
        self.test_params = params = dict(parse_console=False)
        cfg = self.test_suite.create_config({})
        for name, value in options(self):
            if value is not None:
                setting = cfg.settings[name]

                if setting.nargs in ('*', '+'):
                    values = []
                    for v in shlex.split(value):
                        values.extend((c for c in v.split(',') if c))
                    value = values

                setattr(self, name, value)
                params[name] = value

        self.test_args = sys.argv[2:]

    def run_tests(self):
        test_suite = self.test_suite(**self.test_params)
        self.result_code = test_suite.start(exit=False)


def options(self):
    for name in self.user_options:
        name = name[0]
        if name.endswith('='):
            name = name[:-1]
        name = name.replace('-', '_')
        yield name, getattr(self, name, None)
