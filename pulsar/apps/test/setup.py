import shlex

import setuptools.command.test as orig

from pulsar.apps.test import TestSuite


class Test(orig.test):
    test_suite = True
    user_options = [
        ('list-labels', 'l', 'List all test labels without performing tests'),
        ('coverage', None, 'Collect code coverage from all spawn actors'),
        ('coveralls', None, 'Publish coverage to coveralls'),
        ('sequential', None, 'Run test functions sequentially'),
        ('log-level=', None, 'Logging level'),
        ('test-plugins=', None, 'Test plugins'),
        ('pulsar-args=', 'a',
         "Additiona arguments to pass to pulsar test suite")]

    def initialize_options(self):
        self.list_labels = None
        self.coverage = None
        self.coveralls = None
        self.sequential = None
        self.log_level = None
        self.test_plugins = None
        self.pulsar_args = None

    def finalize_options(self):
        for name in ('log_level', 'test_plugins', 'pulsar_args'):
            value = getattr(self, name)
            if value:
                setattr(self, name, shlex.split(value))
        self.test_args = self.pulsar_args or []

    def run_tests(self):
        test_suite = TestSuite(list_labels=self.list_labels,
                               verbosity=self.verbose+1,
                               coverage=self.coverage,
                               coveralls=self.coveralls,
                               sequential=self.sequential,
                               test_plugins=self.test_plugins,
                               log_level=self.log_level,
                               argv=self.test_args)
        test_suite.start()
