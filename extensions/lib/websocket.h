#ifndef     __PULSAR_WEBSOCKET__
#define     __PULSAR_WEBSOCKET__

#include <Python.h>


PyObject* websocket_mask(const char* chunk, const char* key,
                         size_t chunk_length, size_t mask_length) {
    size_t i;
    char* buf;
    PyObject* result = PyBytes_FromStringAndSize(NULL, chunk_length);
    if (!result) {
        return NULL;
    }
    buf = PyBytes_AS_STRING(result);
    for (i = 0; i < chunk_length; i++) {
        buf[i] = chunk[i] ^ key[i % mask_length];
    }
    return result;
}


#endif  //  __PULSAR_WEBSOCKET__
