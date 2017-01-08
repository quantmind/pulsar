#ifndef     __PULSAR_HTTP__
#define     __PULSAR_HTTP__

#include <stdio.h>
#include <time.h>
#include <Python.h>


inline PyObject* _http_date(time_t timestamp) {
    char buf[1000];
    struct tm gmt = *gmtime(&timestamp);
    strftime(buf, sizeof buf, "%a, %d %b %Y %H:%M:%S GMT", &gmt);
    return PyUnicode_FromString(&buf);
}


#endif  //  __PULSAR_HTTP__
