import sys

cdef extern from "math.h":
    double log(double x)
    double sqrt(double x)

cdef double clog2 = log(2.)

cdef inline int int_max(int a, int b): return a if a >= b else b
cdef inline int int_min(int a, int b): return a if a >= b else b

# MSVC does not have log2!
cdef inline double Log2(double x):
    return log(x) / clog2

ispy3k = sys.version_info >= (3, 0)

if ispy3k:
    string_type = str
    strict_string_type = str
else:
    string_type = basestring
    strict_string_type = unicode
    range = xrange


cdef inline bytes to_bytes(object value, str encoding):
    if isinstance(value, bytes):
        return value
    elif isinstance(value, string_type):
        return value.encode(encoding)
    else:
        raise TypeError('Requires bytes or string')


cdef inline object native_str(object value):
    if ispy3k:
        if isinstance(value, bytes):
            return value.decode('utf-8')
        else:
            return value
    else:
        if isinstance(value, unicode):
            return value.encode('utf-8')
        else:
            return value
