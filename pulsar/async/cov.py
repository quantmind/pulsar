from multiprocessing import current_process

try:
    import coverage
    from coverage.monkey import patch_multiprocessing
except ImportError:
    coverage = None


class Coverage:
    '''Coverage mixin for actors.
    '''
    @property
    def coverage(self):
        return getattr(current_process(), '_coverage', None)

    def start_coverage(self):
        if self.is_arbiter() and self.cfg.coverage:
            if not coverage:
                self.logger.error('Coverage module not installed. '
                                  'Cannot start coverage.')
                return
            cov = self.coverage
            if not cov:
                self.logger.warning('Start coverage')
                p = current_process()
                p._coverage = coverage.Coverage(data_suffix=True)
                patch_multiprocessing()
                p._coverage.start()

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
