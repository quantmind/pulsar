    
cdef extern from "parser.h":
    
    cdef cppclass RedisParser:
        RedisParser(object, object) except +
        void feed(const char*, long)
        object get()
        object get_buffer()
        void set_encoding(const char*)
        
    object pack_command(object)