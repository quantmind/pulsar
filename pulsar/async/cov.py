import os

from multiprocessing import current_process

try:
    import coverage
except ImportError:
    coverage = None


class Coverage:
    '''Coverage mixin for actors.
    '''
    @property
    def coverage(self):
        return getattr(current_process(), '_coverage', None)

    def start_coverage(self):
        if self.cfg.coverage:
            if not coverage:
                self.logger.error('Coverage module not installed. '
                                  'Cannot start coverage.')
                return
            if self.is_arbiter():
                if not self.coverage:
                    self.logger.warning('Start coverage')
                    p = current_process()
                    p._coverage = coverage.Coverage(data_suffix=True)
                    coverage.process_startup()
                    p._coverage.start()
                config_file = self.coverage.config_file
                os.environ['COVERAGE_PROCESS_START'] = config_file
            elif self.cfg.concurrency == 'subprocess':
                coverage.process_startup()

    def stop_coverage(self):
        cov = self.coverage
        if cov and self.is_arbiter():
            self.logger.warning('Saving coverage file')
            cov.stop()
            cov.save()
            c = coverage.Coverage()
            c.combine()
            c.save()
            self.stream.write(
                'Coverage file available. Type "coverage html" '
                'for a report\n')
