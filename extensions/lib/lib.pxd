
cdef extern from "math.h":
    double log(double x)
    double sqrt(double x)


cdef double clog2 = log(2.)

cdef inline int int_max(int a, int b): return a if a >= b else b
cdef inline int int_min(int a, int b): return a if a >= b else b


# MSVC does not have log2!
cdef inline double Log2(double x):
    return log(x) / clog2
