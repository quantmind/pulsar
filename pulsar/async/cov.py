import os
import tempfile
import shutil

try:
    import coverage
except ImportError:     # pragma    nocover
    coverage = None


class Coverage(object):
    '''Coverage mixin for actors.
    '''
    _coverage = None

    def setup_coverage(self):
        if self.cfg.coverage:  # create the coverage directory
            cwd = os.getcwd()
            self.params.coverage_directory = tempfile.mkdtemp(dir=cwd)
            self.params.coverage_target = os.environ.pop(
                'COVERAGE_FILE', os.path.join(cwd, '.coverage.pulsar'))

    def start_coverage(self):
        if coverage and self.cfg.coverage:
            target = os.environ.get('COVERAGE_FILE') or '.coverage'
            file_name = os.path.join(self.params.coverage_directory,
                                     '.coverage')
            self.logger.info('start coverage')
            self._coverage = coverage.coverage(data_file=file_name,
                                               data_suffix=True)
            self._coverage.start()

    def stop_coverage(self):
        if self._coverage and not self.is_arbiter():
            self.logger.info('Saving coverage file in %s',
                             self.params.coverage_directory)
            self._coverage.stop()
            self._coverage.save()

    def collect_coverage(self):
        if self._coverage:
            self.logger.info('Combining coverage files in to %s',
                             self.params.coverage_target)
            self._coverage.stop()
            self._coverage.save()
            file_name = os.path.join(self.params.coverage_directory,
                                     '.coverage')
            c = coverage.coverage(data_file=file_name, data_suffix=True)
            c.combine()
            c.save()
            shutil.move(file_name, self.params.coverage_target)
            shutil.rmtree(self.params.coverage_directory)
