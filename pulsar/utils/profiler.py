import cProfile
import pstats


class Profiler:

    def __init__(self, sortby=None):
        self.profiler = None
        self.sortby = sortby or ('cumtime',)

    def __enter__(self):
        self.profiler = cProfile.Profile()
        self.profiler.enable()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.profiler.disable()
        self.write_stats()

    def write_stats(self):
        p = pstats.Stats(self.profiler)
        p.strip_dirs().sort_stats(*self.sortby).print_stats()
