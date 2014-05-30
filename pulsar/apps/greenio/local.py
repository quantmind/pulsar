"""Greenlet-local objects.

This module is based on ``threading_local.py`` from the standard library.
"""
from weakref import ref
from contextlib import contextmanager
from threading import RLock

from greenlet import getcurrent


class _localimpl:
    """A class managing greenelt-local dicts"""
    __slots__ = ('key', 'dicts', 'localargs', 'locallock', '__weakref__')

    def __init__(self):
        # The key used in the Thread objects' attribute dicts.
        # We keep it a string for speed but make it unlikely to clash with
        # a "real" attribute.
        self.key = '_greenlet_local._localimpl.' + str(id(self))
        # { id(Thread) -> (ref(Thread), thread-local dict) }
        self.dicts = {}

    def get_dict(self):
        """Return the dict for the current greenlet. Raises KeyError if none
        defined."""
        greenlet = getcurrent()
        return self.dicts[id(greenlet)][1]

    def create_dict(self):
        """Create a new dict for the current greenlet, and return it."""
        localdict = {}
        key = self.key
        greenlet = getcurrent()
        idt = id(greenlet)

        def local_deleted(_, key=key):
            # When the localimpl is deleted, remove the greenlet attribute.
            greenlet = wrgreenlet()
            if greenlet is not None:
                del greenlet.__dict__[key]

        def greenlet_deleted(_, idt=idt):
            # When the greenlet is deleted, remove the local dict.
            # Note that this is suboptimal if the greenlet object gets
            # caught in a reference loop. We would like to be called
            # as soon as the OS-level greenlet ends instead.
            local = wrlocal()
            if local is not None:
                dct = local.dicts.pop(idt)

        wrlocal = ref(self, local_deleted)
        wrgreenlet = ref(greenlet, greenlet_deleted)
        greenlet.__dict__[key] = wrlocal
        self.dicts[idt] = wrgreenlet, localdict
        return localdict


@contextmanager
def _patch(self):
    impl = object.__getattribute__(self, '_local__impl')
    try:
        dct = impl.get_dict()
    except KeyError:
        dct = impl.create_dict()
        args, kw = impl.localargs
        self.__init__(*args, **kw)
    with impl.locallock:
        object.__setattr__(self, '__dict__', dct)
        yield


class local(object):
    __slots__ = ('_local__impl', '__dict__')

    def __new__(cls, *args, **kw):
        if (args or kw) and (cls.__init__ is object.__init__):
            raise TypeError("Initialization arguments are not supported")
        self = object.__new__(cls)
        impl = _localimpl()
        impl.localargs = (args, kw)
        impl.locallock = RLock()
        object.__setattr__(self, '_local__impl', impl)
        # We need to create the thread dict in anticipation of
        # __init__ being called, to make sure we don't call it
        # again ourselves.
        impl.create_dict()
        return self

    def __getattribute__(self, name):
        with _patch(self):
            return object.__getattribute__(self, name)

    def __setattr__(self, name, value):
        if name == '__dict__':
            raise AttributeError(
                "%r object attribute '__dict__' is read-only"
                % self.__class__.__name__)
        with _patch(self):
            return object.__setattr__(self, name, value)

    def __delattr__(self, name):
        if name == '__dict__':
            raise AttributeError(
                "%r object attribute '__dict__' is read-only"
                % self.__class__.__name__)
        with _patch(self):
            return object.__delattr__(self, name)
