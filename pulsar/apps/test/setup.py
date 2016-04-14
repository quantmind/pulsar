import shlex

import setuptools.command.test as orig

from pulsar.utils.slugify import slugify
from pulsar.apps.test import TestSuite


class Test(orig.test):
    test_suite = True
    list_options = set(['log-level=', 'test-plugins=',
                        'test-modules=', 'pulsar-args='])
    user_options = [
        ('list-labels', 'l', 'List all test labels without performing tests'),
        ('coverage', None, 'Collect code coverage from all spawn actors'),
        ('coveralls', None, 'Publish coverage to coveralls'),
        ('sequential', None, 'Run test functions sequentially'),
        ('test-timeout=', None, 'Timeout for asynchronous tests'),
        ('log-level=', None, 'Logging level'),
        ('test-plugins=', None, 'Test plugins'),
        ('test-modules=', None, 'Modules where to look for tests'),
        ('pulsar-args=', 'a',
         "Additional arguments to pass to pulsar test suite")]

    def initialize_options(self):
        for name, _, _ in self.user_options:
            setattr(self, slugify(name, '_'), None)

    def finalize_options(self):
        self.test_params = {}
        for name, _, _ in self.user_options:
            attr = slugify(name, '_')
            value = getattr(self, attr)
            if value and name in self.list_options:
                value = shlex.split(value)
                setattr(self, attr, value)
            if value is not None:
                self.test_params[attr] = value
        self.test_args = self.pulsar_args or []

    def run_tests(self):
        test_suite = TestSuite(verbosity=self.verbose+1,
                               argv=self.test_args,
                               **self.test_params)
        test_suite.start()
