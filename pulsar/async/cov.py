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
        if not self.cfg.coverage:
            return
        if not coverage:
            self.logger.error('Coverage module not installed. '
                              'Cannot start coverage.')
            return
        cov = self.coverage
        if not cov:
            self.logger.warning('Start coverage')
            p = current_process()
            p._coverage = coverage.Coverage(data_suffix=True)
            coverage.process_startup()
            p._coverage.start()

    def stop_coverage(self):
        cov = self.coverage
        if cov:
            self.logger.warning('Saving coverage file')
            cov.stop()
            cov.save()
            if self.is_arbiter():
                c = coverage.Coverage()
                c.combine()
                c.save()
                self.stream.write(
                    'Coverage file available. Type "coverage html" '
                    'for a report\n')
