import os
import logging
from shutil import rmtree

try:
    import coverage
except ImportError:
    coverage = None


from pulsar import Setting
from pulsar.apps import test

from .profile import absolute_file


LOGGER = logging.getLogger('pulsar.test')


class Coverage(test.TestPlugin):
    ''':class:`pulsar.apps.test.Plugin` for code coverage.'''
    desc = '''Test coverage with coverage'''
    coverage_config = Setting(flags=['--coverage-config'],
                              default='.coveragerc',
                              desc='''location of coverage config file''',
                              validator=absolute_file)
    coverage_path = Setting(flags=['--coverage_path'],
                            default='htmlcov',
                            desc='''location of coverage directory''',
                            validator=absolute_file)
    coverage = None
    dirname = 'cfiles'

    def on_start(self):
        if coverage and self.config.coverage:
            if os.path.isdir(self.dirname):
                rmtree(self.dirname)
            os.mkdir(self.dirname)

    def startTestClass(self, testcls):
        if coverage and self.config.coverage:
            cps = self.config.coverage_config
            name = '%s/.%s.%s' % (self.dirname, testcls.__module__,
                                  testcls.__name__)
            os.environ['COVERAGE_PROCESS_START'] = cps
            self.coverage = coverage.coverage(data_file=name, config_file=cps)
            #self.coverage.atexit_registered = True
            self.coverage.start()

    def stopTestClass(self, testcls):
        if self.coverage:
            self.coverage.stop()
            self.coverage.save()
            #self.coverage.html_report(directory=self.config.coverage_path)
            #LOGGER.info('Coverage html report at %s',
            #            self.config.coverage_path)
