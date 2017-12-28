
cdef extern from "math.h":
    double log(double x)
    double sqrt(double x)

cdef double clog2 = log(2.)

cdef inline int int_max(int a, int b):
    return a if a >= b else b


cdef inline int int_min(int a, int b):
    return a if a >= b else b


# MSVC does not have log2!
cdef inline double Log2(double x):
    return log(x) / clog2


cdef class Event:
    cdef readonly str name
    cdef int _onetime
    cdef list _handlers
    cdef object _self
    cdef object _waiter
    cpdef object onetime(self)
    cpdef object fired(self)
    cpdef list handlers(self)
    cpdef bind(self, object callback)
    cpdef clear(self)
    cpdef int unbind(self, object callback)
    cpdef fire(self, exc=?, data=?)
    cpdef object waiter(self)


cdef class EventHandler:
    cdef dict _events
    cpdef dict events(self)
    cpdef Event event(self, str name)
    cpdef copy_many_times_events(self, EventHandler other)
    cpdef bind_events(self, dict events)
    cpdef fire_event(self, str name, exc=?, data=?)
