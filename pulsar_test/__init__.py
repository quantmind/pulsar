import shlex
from multiprocessing import current_process

import setuptools.command.test as orig


OPTION_MAP = {
    'io': 'event_loop'
}


def extend(values, extra):
    extra.extend(values)
    return extra


class Test(orig.test):
    test_suite = True
    start_coverage = False
    list_options = set([
        'log-level=',
        'test-plugins=',
        'test-modules=',
        'pulsar-args='
    ])
    user_options = [
        ('list-labels', 'l', 'List all test labels without performing tests'),
        ('coverage', None, 'Collect code coverage from all spawn actors'),
        ('coveralls', None, 'Publish coverage to coveralls'),
        ('sequential', None, 'Run test functions sequentially'),
        ('test-timeout=', None, 'Timeout for asynchronous tests'),
        ('log-level=', None, 'Logging level'),
        ('io=', None, 'Event Loop'),
        ('concurrency=', None, 'Concurrency'),
        ('test-plugins=', None, 'Test plugins'),
        ('test-modules=', None, 'Modules where to look for tests'),
        ('pulsar-args=', 'a',
         "Additional arguments to pass to pulsar test suite")]

    def initialize_options(self):
        for name, _, _ in self.user_options:
            setattr(self, self._slugify(name), None)

    def finalize_options(self):
        self.test_params = {}
        for name, _, _ in self.user_options:
            attr = self._slugify(name)
            value = getattr(self, attr)
            if value and name in self.list_options:
                value = shlex.split(value)
                setattr(self, attr, value)
            if value is not None:
                param_name = OPTION_MAP.get(attr, attr)
                self.test_params[param_name] = value
        self.test_args = self.pulsar_args or []

    def run_tests(self):
        if self.coverage and self.start_coverage:
            import coverage
            p = current_process()
            p._coverage = coverage.Coverage(data_suffix=True)
            coverage.process_startup()
            p._coverage.start()

        from pulsar.apps.test import TestSuite
        params = self.get_test_parameters()
        test_suite = TestSuite(argv=self.test_args,
                               **params)
        test_suite.start()

    def get_test_parameters(self):
        params = self.test_params
        params['verbosity'] = self.verbose+1
        return params

    def _slugify(self, name):
        return name.replace('-', '_').replace('=', '')


class Bench(Test):
    description = 'Run benchmarks with pulsar benchmark test suite'
    user_options = extend(Test.user_options, [
        ('repeat=', None, 'Number of repetitions'),
    ])

    def get_test_parameters(self):
        params = self.test_params
        params['benchmark'] = True
        params['verbosity'] = False
        if 'test_timeout' not in params:
            params['test_timeout'] = 300
        return params
