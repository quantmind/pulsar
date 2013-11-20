#ifndef     __PULSAR_WEBSOCKET__
#define     __PULSAR_WEBSOCKET__

#include <Python.h>


inline const char *websocket_mask(const char* chunk, const char* key,
        size_t chunk_length, size_t mask_length) {
    char *buf = (char*)chunk;
    size_t i;
    for (i = 0; i < chunk_length; i++) {
        buf[i] = chunk[i] ^ key[i % mask_length];
    }
    return buf;
}


#endif  //  __PULSAR_WEBSOCKET__
