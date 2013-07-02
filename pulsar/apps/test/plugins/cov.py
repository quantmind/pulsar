import os
import logging

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
    
    def on_start(self):
        if coverage and self.config.coverage:
            cps = self.config.coverage_config
            os.environ['COVERAGE_PROCESS_START'] = cps
            self.coverage = coverage.coverage(config_file=cps, auto_data=True)
            self.coverage.atexit_registered = True
            self.coverage.start()
            
    def on_end(self):
        if self.coverage:
            LOGGER.info('Save coverage files')
            self.coverage.save()
            self.coverage.stop()
            self.coverage.html_report(directory=self.config.coverage_path)
            LOGGER.info('Coverage html report at %s', self.config.coverage_path)
                