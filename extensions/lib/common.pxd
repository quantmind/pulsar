
cdef extern from "parser.h":

    cdef cppclass RedisParser:
        RedisParser(object, object) except +
        void feed(const char*, long)
        object get()
        object get_buffer()
        void set_encoding(const char*)

    object pack_command(object)


cdef extern from "websocket.h":
    object websocket_mask(const char* chunk, const char* key,
                          size_t chunk_length, size_t mask_length)
