import os

try:
    import coverage
except ImportError:
    coverage = None


class Coverage(object):
    '''Coverage mixin for actors.
    '''
    _coverage = None

    def start_coverage(self):
      if coverage and self.cfg.coverage:
          file_name = os.path.join(self.params.coverage_directory, 'pulsar')
          self._coverage = coverage.coverage(data_file=file_name,
                               data_suffix=True)
          self._coverage.start()

    def stop_coverage(self):
        if self._coverage:
            self._coverage.stop()
            self._coverage.save()

    def collect_coverage(self):
        if self._coverage:
            self._coverage.combine()
