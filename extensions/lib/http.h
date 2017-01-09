#ifndef     __PULSAR_HTTP__
#define     __PULSAR_HTTP__

#include <stdio.h>
#include <time.h>
#include <Python.h>


static char  *week[] = { "Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat" };
static char  *months[] = { "Jan", "Feb", "Mar", "Apr", "May", "Jun",
                           "Jul", "Aug", "Sep", "Oct", "Nov", "Dec" };

inline PyObject* _http_date(time_t timestamp) {
    char buf[50];
    struct tm gmt = *gmtime(&timestamp);
    //strftime(buf, sizeof buf, "%a, %d %b %Y %H:%M:%S GMT", &gmt);
    sprintf(buf, "%s, %02d %s %4d %02d:%02d:%02d GMT",
                       week[gmt.tm_wday], gmt.tm_mday,
                       months[gmt.tm_mon], gmt.tm_year + 1900,
                       gmt.tm_hour, gmt.tm_min, gmt.tm_sec);
    return PyUnicode_FromString(&buf);
}


#endif  //  __PULSAR_HTTP__
