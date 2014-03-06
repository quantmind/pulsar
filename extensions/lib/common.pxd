
cdef extern from "websocket.h":
    object websocket_mask(const char* chunk, const char* key,
                          size_t chunk_length, size_t mask_length)
