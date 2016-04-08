
class Coverage:
    '''Coverage mixin for actors.
    '''
    @property
    def coverage(self):
        return None

    def start_coverage(self):
        pass

    def stop_coverage(self):
        pass

    def collect_coverage(self):
        pass
