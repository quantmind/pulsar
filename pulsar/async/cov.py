from multiprocessing import current_process

try:
    import coverage
except ImportError:
    coverage = None


class Coverage(object):
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
            cov = self.coverage
            if not cov:
                self.logger.info('Start coverage')
                p = current_process()
                p._coverage = coverage.coverage(data_suffix=True)
                p._coverage.start()

    def stop_coverage(self):
        cov = self.coverage
        if cov and not self.is_arbiter():
            self.logger.info('Saving coverage file')
            cov.stop()
            cov.save()

    def collect_coverage(self):
        cov = self.coverage
        if cov:
            self.logger.info('Combining coverage files')
            cov.stop()
            cov.save()
            c = coverage.coverage(data_suffix=True)
            c.combine()
            c.save()
            self.stream.write('Coverage file available. Type "coverage html" '
                              'for a report\n')
