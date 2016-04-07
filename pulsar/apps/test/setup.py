import sys
import os
import shlex
from multiprocessing import current_process
from importlib import import_module

import setuptools.command.test as orig

if '--coverage' in sys.argv:
    import coverage
    p = current_process()
    p._coverage = coverage.coverage(data_suffix=True)
    p._coverage.start()


elif '--coveralls' in sys.argv:
    import pulsar
    from pulsar.utils.path import Path
    from pulsar.apps.test.cov import coveralls

    repo_token = None
    strip_dirs = [Path(pulsar.__file__).parent.parent, os.getcwd()]
    if os.path.isfile('.coveralls-repo-token'):
        with open('.coveralls-repo-token') as f:
            repo_token = f.read().strip()
    coveralls(strip_dirs=strip_dirs, repo_token=repo_token)
    sys.exit(0)


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
        self.result_code = test_suite.start()


def options(self):
    for name in self.user_options:
        name = name[0]
        if name.endswith('='):
            name = name[:-1]
        name = name.replace('-', '_')
        yield name, getattr(self, name, None)
