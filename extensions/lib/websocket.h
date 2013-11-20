#ifndef     __PULSAR_WEBSOCKET__
#define     __PULSAR_WEBSOCKET__


inline PyObject* websocket_mask(PyObject* self, PyObject* args) {
    const char* mask;
    int mask_len;
    const char* data;
    int data_len;
    int i;

    if (!PyArg_ParseTuple(args, "s#s#", &mask, &mask_len, &data, &data_len)) {
        return NULL;
    }

    PyObject* result = PyBytes_FromStringAndSize(NULL, data_len);
    if (!result) {
        return NULL;
    }
    char* buf = PyBytes_AsString(result);
    for (i = 0; i < data_len; i++) {
        buf[i] = data[i] ^ mask[i % 4];
    }

    return result;
}



#endif      __PULSAR_WEBSOCKET__
