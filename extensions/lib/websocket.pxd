
cdef extern from "websocket.h":
    object websocket_mask(const char* chunk, const char* key,
                          size_t chunk_length, size_t mask_length)


cdef inline bytes to_bytes(object value, str encoding):
    if isinstance(value, bytes):
        return value
    elif isinstance(value, str):
        return value.encode(encoding)
    else:
        raise TypeError('Requires bytes or string')
