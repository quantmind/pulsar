#ifndef __PULSAR_UTILS_H__
#define __PULSAR_UTILS_H__

#include <Python.h>
#include <cstdlib>
#include <string>
#include <sstream>
#include <list>

typedef std::string string;
typedef long long integer;
typedef std::list<PyObject*>    stdpylist;

#if PY_MAJOR_VERSION == 2
    #define BYTES_FORMAT "s#"

    inline string to_bytes(PyObject* value) {
        if (PyFloat_Check(value)) {
            value = PyObject_Repr(value);
        } else if (PyUnicode_Check(value)) {
            value = PyUnicode_AsUTF8String(value);
        } else if (!PyString_Check(value)) {
            value = PyObject_Str(value);
        }
        return string(PyString_AS_STRING(value), PyString_GET_SIZE(value));
    }

    inline PyObject* to_py_string(const string& value) {
        return PyBytes_FromStringAndSize(value.c_str(), value.size());
    }
#else
    #define BYTES_FORMAT "y#"

    inline string to_bytes(PyObject* value) {
        if (PyFloat_Check(value)) {
            value = PyUnicode_AsUTF8String(PyObject_Repr(value));
        } else if (!PyBytes_Check(value)) {
            value = PyUnicode_AsUTF8String(PyObject_Str(value));
        }
        return string(PyBytes_AS_STRING(value), PyBytes_GET_SIZE(value));
    }

    inline PyObject* to_py_string(const string& value) {
        return PyUnicode_FromStringAndSize(value.c_str(), value.size());
    }
#endif

inline PyObject* to_py_bytes(const string& value) {
    return PyBytes_FromStringAndSize(value.c_str(), value.size());
}

#endif	//	__PULSAR_UTILS_H__
