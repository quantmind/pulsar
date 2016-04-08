import sys
import shlex
import json
import subprocess

import setuptools.command.test as orig


class PulsarTest(orig.test):
    test_suite = True
    user_options = [
        ('list-labels', 'l', 'List all test labels without performing tests'),
        ('coverage', None, 'Collect code coverage from all spawn actors'),
        ('pulsar-args=', 'a', "Arguments to pass to pulsar.test")]

    def initialize_options(self):
        self.list_labels = None
        self.coverage = None
        self.pulsar_args = None

    def finalize_options(self):
        if self.pulsar_args:
            argv = shlex.split(self.pulsar_args)
        else:
            argv = []
        self.test_args = argv

    def run_tests(self):
        from pulsar.apps.test import TestSuite
        test_suite = TestSuite(list_labels=self.list_labels,
                               verbosity=self.verbose+1,
                               coverage=self.coverage,
                               argv=self.test_args)
        self.result_code = test_suite.start(exit=False)


def extend(params, package=None):
    cmdclass = params.get('cmdclass', {})
    cmdclass['test'] = PulsarTest
    params['cmdclass'] = cmdclass
    #
    if package:
        meta = sh('%s %s %s' % (sys.executable, __file__, package))
        params.update(json.loads(meta))


def sh(command, cwd=None):
    return subprocess.Popen(command,
                            stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE,
                            shell=True,
                            cwd=cwd,
                            universal_newlines=True).communicate()[0]

if __name__ == '__main__':
    if len(sys.argv) == 2:
        package = sys.argv[1]
        pkg = __import__(package)
        print(json.dumps(dict(version=pkg.__version__,
                              description=pkg.__doc__)))
