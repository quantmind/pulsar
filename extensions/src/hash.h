/* Copyright (c) 2013, Luca Sbardella <luca dot sbardella at gmail dot com>
 *
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the name of Redis nor the names of its contributors may be used
 *     to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */
#ifndef __COMPACT_HASH_H__
#define __COMPACT_HASH_H__

#include "utils.h"


template <class Type>
class Hash {
public:
    Hash(PyObject* args, PyObject* kwargs);

    PyObject* get(PyObject* value) const;

    void update(PyObject* args, PyObject* kwargs);

    size_t size() const {return _map.size();}
private:
    PyObject*   _data;
    int _merge(PyObject*);
    int _merge_from_seq(PyObject*);
};


template<class Type>
inline Hash<Type>::Hash(PyObject* args, PyObject* kwargs):_data(NULL) {
    update(args, kwargs);
}


template <>
inline PyObject* Hash<PyObject*>::get(PyObject* value) const {
    string key = to_bytes(value);
    const_iterator it = _map.find(key);
    if (it !=_map.end()) {
        return it->second;
    } else {
        PyErr_SetObject(PyExc_KeyError, to_py_string(key));
        return NULL;
    }
}


template <class Type>
inline void Hash<Type>::update(PyObject* args, PyObject* kwargs) {
    size_t s = PySequence_Length(args);
    if (s == 1) {
        PyObject* iterable = PySequence_GetItem(args, 0);
        if (PyMapping_Check(iterable)) {
            _merge(iterable);
        } else if (PyIter_Check(iterable)) {
            _merge_from_seq(iterable);
        } else {
            string err("object is not iterable");
            PyErr_SetObject(PyExc_TypeError, to_py_string(err));
            return;
        }
    } else if (s) {
        string err("object is not iterable");
        PyErr_SetObject(PyExc_TypeError, to_py_string(err));
        return;
    }
    _merge(kwargs);
}


template <>
inline int Hash<PyObject*>::_merge(PyObject* mapping) {
    PyObject *keys = PyMapping_Keys(mapping);
    PyObject *iter;
    PyObject *key, *value;
    int status;

    if (keys == NULL)
        return -1;

    iter = PyObject_GetIter(keys);
    Py_DECREF(keys);
    if (iter == NULL)
        return -1;

    for (key = PyIter_Next(iter); key; key = PyIter_Next(iter)) {
        value = PyObject_GetItem(mapping, key);
        if (value == NULL) {
            Py_DECREF(iter);
            Py_DECREF(key);
            return -1;
        }
        _map.insert(value_type(to_bytes(key), value));
    }
    Py_DECREF(iter);
    if (PyErr_Occurred())
        return -1;
}

template <>
inline int Hash<PyObject*>::_merge_from_seq(PyObject* sequence) {
    return -1;
}


#define PERTURB_SHIFT 5


typedef Hash<PyObject*>     HashObject;
#endif  //  __COMPACT_HASH_H__
