from multiprocessing import dummy, current_process


class Thread(dummy.DummyProcess):
    _loop = None
    _pool_loop = None

    @property
    def pid(self):
        return current_process().pid

    def terminate(self):
        '''Invoke the stop on the event loop method.'''
        if self.is_alive() and self._loop:
            self._loop.call_soon_threadsafe(self._loop.stop)

    def set_loop(self, loop):
        assert self._loop is None
        self._loop = loop
