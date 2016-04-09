import shlex

import setuptools.command.test as orig

from pulsar.apps.test import TestSuite


class Test(orig.test):
    test_suite = True
    user_options = [
        ('list-labels', 'l', 'List all test labels without performing tests'),
        ('coverage', None, 'Collect code coverage from all spawn actors'),
        ('log-level=', None, 'Logging level'),
        ('pulsar-args=', 'a', "Arguments to pass to pulsar.test")]

    def initialize_options(self):
        self.list_labels = None
        self.coverage = None
        self.log_level = None
        self.pulsar_args = None

    def finalize_options(self):
        if self.log_level:
            self.log_level = shlex.split(self.log_level)
        if self.pulsar_args:
            argv = shlex.split(self.pulsar_args)
        else:
            argv = []
        self.test_args = argv

    def run_tests(self):
        test_suite = TestSuite(list_labels=self.list_labels,
                               verbosity=self.verbose+1,
                               coverage=self.coverage,
                               log_level=self.log_level,
                               argv=self.test_args)
        self.result_code = test_suite.start(exit=False)
