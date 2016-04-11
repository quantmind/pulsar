import os
import sys
import shlex
import json
import subprocess
from multiprocessing import current_process

import setuptools.command.test as orig


class PulsarTest(orig.test):
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
        if self.coverage:
            import coverage
            from coverage.monkey import patch_multiprocessing
            print('Collect coverage')
            p = current_process()
            p._coverage = coverage.Coverage(data_suffix=True)
            patch_multiprocessing()
            p._coverage.start()

        from pulsar.apps.test import TestSuite
        test_suite = TestSuite(list_labels=self.list_labels,
                               verbosity=self.verbose+1,
                               coverage=self.coverage,
                               coveralls=self.coveralls,
                               sequential=self.sequential,
                               test_plugins=self.test_plugins,
                               log_level=self.log_level,
                               argv=self.test_args)
        test_suite.start()


def extend(params, package=None):
    cmdclass = params.get('cmdclass', {})
    cmdclass['test'] = PulsarTest
    params['cmdclass'] = cmdclass
    #
    if package:
        path = os.path.abspath(os.getcwd())
        meta = sh('%s %s %s %s' % (sys.executable, __file__, package, path))
        params.update(json.loads(meta))

    return params


def read(name):
    with open(name) as fp:
        return fp.read()


def requirements(name):
    install_requires = []
    dependency_links = []

    for line in read(name).split('\n'):
        if line.startswith('-e '):
            link = line[3:].strip()
            if link == '.':
                continue
            dependency_links.append(link)
            line = link.split('=')[1]
        line = line.strip()
        if line:
            install_requires.append(line)

    return install_requires, dependency_links


def sh(command, cwd=None):
    return subprocess.Popen(command,
                            stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE,
                            shell=True,
                            cwd=cwd,
                            universal_newlines=True).communicate()[0]

if __name__ == '__main__':
    if len(sys.argv) >= 2:
        package = sys.argv[1]
        if len(sys.argv) > 2:
            sys.path.append(sys.argv[2])
        pkg = __import__(package)
        print(json.dumps(dict(version=pkg.__version__,
                              description=pkg.__doc__)))
