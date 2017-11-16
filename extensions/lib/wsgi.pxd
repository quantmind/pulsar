
cdef extern from "http.h":
    ctypedef int time_t
    object _http_date(time_t timestamp)
