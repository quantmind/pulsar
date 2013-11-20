include "common.pyx"

cimport lua
from lua cimport lua_State

cimport cpython
cimport cpython.ref
cimport cpython.tuple

from threading import RLock

cdef extern from *:
    ctypedef char* const_char_ptr "const char*"

DEF POBJECT = "POBJECT" # as used by LunaticPython

class LuaError(Exception):
    """Base class for errors in the Lua runtime.
    """
    pass


class LuaSyntaxError(LuaError):
    """Syntax error in Lua code.
    """
    pass


cdef class Lua:
    """The main entry point to the Lua runtime.
    """
    cdef lua_State *state
    cdef tuple raised_exception
    cdef str encoding
    cdef object lock
    cdef dict scripts

    def __cinit__(self, encoding=None, str python_lib=None):
        self.state = lua.luaL_newstate()
        if self.state is NULL:
            raise LuaError("Failed to initialise Lua runtime")
        self.scripts = {}
        self.encoding = encoding
        self.lock = RLock()
        lua.luaL_openlibs(self.state)
        _init_python_lib(self, python_lib or 'python')
        lua.lua_settop(self.state, 0)

    def __dealloc__(self):
        if self.state is not NULL:
            lua.lua_close(self.state)
            self.state = NULL

    def version(self):
        '''Lua version number'''
        return native_str(lua.LUA_VERSION)

    def execute(self, lua_code, name=None):
        """Execute a Lua script passed in a string.
        """
        return _run_lua(self, to_bytes(lua_code, self.encoding))

    def
    def __enter__(self):
        self.lock.acquire()

    def __exit__(self, *args):
        lua.lua_settop(self.state, 0)
        self.lock.release()


cdef class LuaObject:
    cdef Lua runtime
    cdef int ref

    def __dealloc__(self):
        if self.runtime:
            with self.runtime.lock:
                if self.ref:
                    lua.luaL_unref(self.runtime.state, lua.LUA_REGISTRYINDEX,
                                   self.ref)
            cpython.ref.Py_DECREF(self.runtime)
            self.runtime = None

    cdef _init(self, Lua runtime, int n):
        self.runtime = runtime
        cpython.ref.Py_INCREF(runtime)
        lua.lua_pushvalue(runtime.state, n)
        self.ref = lua.luaL_ref(runtime.state, lua.LUA_REGISTRYINDEX)

    def lua(self):
        return self.runtime


cdef class LuaTable(LuaObject):

    def __iter__(self):
        return LuaIter(self, 1)

    def values(self):
        """Returns an iterator over the values of a table that this
        object represents.
        """
        return LuaIter(self, 2)

    def items(self):
        """Returns an iterator over the key-value pairs of a table
        that this object represents.
        """
        return LuaIter(self, 3)


cdef class LuaIter:
    cdef Lua runtime
    cdef int ref
    cdef LuaObject obj
    cdef int what

    def __cinit__(self, LuaTable obj, int what):
        assert obj.runtime is not None
        self.obj = obj
        self.runtime = obj.runtime
        cpython.ref.Py_INCREF(self.runtime)
        self.ref = 0
        self.what = what

    def __iter__(self):
        return self

    def __next__(self):
        cdef lua_State *L
        if self.obj is None or self.runtime is None:
            raise StopIteration
        L = self.runtime.state
        with self.runtime:
            lua.lua_rawgeti(L, lua.LUA_REGISTRYINDEX, self.obj.ref)
            if not lua.lua_istable(L, -1):
                if lua.lua_isnil(L, -1):
                    lua.lua_pop(L, 1)
                    raise LuaError("lost reference")
                raise TypeError('Cannot iterate over not-table')
            if not self.ref:
                # initial key
                lua.lua_pushnil(L)
            else:
                lua.lua_rawgeti(L, lua.LUA_REGISTRYINDEX, self.ref)
            if lua.lua_next(L, -2):
                try:
                    if self.what == 1:
                        retval = _py_from_lua(self.runtime, -2)
                    elif self.what == 2:
                        retval = _py_from_lua(self.runtime, -1)
                    else:
                        retval = (_py_from_lua(self.runtime, -2),
                                  _py_from_lua(self.runtime, -1))
                finally:
                    # pop value
                    lua.lua_pop(L, 1)
                    # pop and store key
                    if not self.ref:
                        self.ref = lua.luaL_ref(L, lua.LUA_REGISTRYINDEX)
                    else:
                        lua.lua_rawseti(L, lua.LUA_REGISTRYINDEX, self.ref)
                return retval
            #
            if self.ref:
                lua.luaL_unref(L, lua.LUA_REGISTRYINDEX, self.ref)
                self.ref = 0
            self.obj = None
        raise StopIteration


# INTERNALS

cdef LuaTable new_table(Lua self, int n):
    cdef LuaTable obj = LuaTable.__new__(LuaTable)
    obj._init(self, n)
    return obj


cdef object _run_lua(Lua self, bytes lua_code):
    cdef int result
    with self:
        if lua.luaL_loadbuffer(self.state,
                               lua_code,
                               len(lua_code),
                               '<python>'):
            raise LuaSyntaxError(
                _lua_error_message(self, "error loading code: %s", -1))
        with nogil:
            result = lua.lua_pcall(self.state, 0, lua.LUA_MULTRET, 0)
        if result:
            _raise_lua_error(self, result)
        return _unpack(self)


cdef str _lua_error_message(Lua self, str err_message, int n):
    cdef size_t size = 0
    cdef const_char_ptr s = lua.lua_tolstring(self.state, n, &size)
    cdef string = s[:size].decode(self.encoding)
    return err_message % string if err_message else string


cdef _raise_lua_error(Lua self, int result):
    if result == lua.LUA_ERRMEM:
        cpython.exc.PyErr_NoMemory()
    else:
        raise LuaError(self._lua_error_message(None, -1))


cdef inline object _unpack(Lua self):
    cdef int nargs = lua.lua_gettop(self.state)
    if nargs == 1:
        return _py_from_lua(self, 1)
    if nargs == 0:
        return None
    return _unpack_multiple(self, nargs)


cdef inline object _py_from_lua(Lua self, int n):
    cdef size_t size = 0
    cdef const_char_ptr s
    cdef float number
    cdef lua_State *L = self.state
    cdef int lua_type = lua.lua_type(L, n)

    if lua_type == lua.LUA_TNIL:
        return None
    elif lua_type == lua.LUA_TNUMBER:
        number = lua.lua_tonumber(L, n)
        if number != <int>number:
            return <double>number
        else:
            return <int>number
    elif lua_type == lua.LUA_TSTRING:
        s = lua.lua_tolstring(L, n, &size)
        return s[:size]
    elif lua_type == lua.LUA_TBOOLEAN:
        return lua.lua_toboolean(L, n)
    elif lua_type == lua.LUA_TTABLE:
        return new_table(self, n)
    else:
        raise LuaError('Lua return type not supported')


cdef tuple _unpack_multiple(Lua self, int nargs):
    cdef tuple args = cpython.tuple.PyTuple_New(nargs)
    cdef int i
    for i in range(nargs):
        arg = _py_from_lua(self, i+1)
        cpython.ref.Py_INCREF(arg)
        cpython.tuple.PyTuple_SET_ITEM(args, i, arg)
    return args


cdef bint call_python(LuaRuntime runtime, lua_State *L, py_object* py_obj) except -1:
    cdef int i, nargs = lua.lua_gettop(L) - 1
    cdef bint ret = 0

    if not py_obj:
        raise TypeError("not a python object")

    cdef tuple args = cpython.tuple.PyTuple_New(nargs)
    for i in range(nargs):
        arg = py_from_lua(runtime, L, i+2)
        cpython.ref.Py_INCREF(arg)
        cpython.tuple.PyTuple_SET_ITEM(args, i, arg)

    return py_to_lua(runtime, L, (<object>py_obj.obj)(*args), 0)


cdef int _init_python_lib(Lua self, str py_lib_name) except -1:
    cdef lua_State *L = self.state
    # create 'python' lib and register our own object metatable
    lua.luaL_openlib(L, py_lib_name, py_lib, 0)
    lua.luaL_newmetatable(L, POBJECT)
    lua.luaL_openlib(L, NULL, py_object_lib, 0)
    lua.lua_pop(L, 1)

    # register global names in the module
    _register_py_object(self, b'Py_None',  b'none', None)
    if register_eval:
        _register_py_object(b'eval',     b'eval', eval)
    _register_py_object(self, b'builtins', b'builtins', builtins)
    return 0 # nothing left to return on the stack


cdef inline int _register_py_object(Lua self, bytes cname, bytes pyname,
                                    object obj) except -1:
    cdef lua_State *L = self.state
    lua.lua_pushlstring(L, cname, len(cname))
    if not py_to_lua_custom(self, obj, 0):
        lua.lua_pop(L, 1)
        raise LuaError("failed to convert %s object" % pyname)
    lua.lua_pushlstring(L, pyname, len(pyname))
    lua.lua_pushvalue(L, -2)
    lua.lua_rawset(L, -5)
    lua.lua_rawset(L, lua.LUA_REGISTRYINDEX)
    return 0


cdef bint _py_to_lua_custom(Lua self, object o, int type_flags):
    cdef py_object *py_obj = <py_object*> lua.lua_newuserdata(L, sizeof(py_object))
    if not py_obj:
        return 0 # values pushed
    cpython.ref.Py_INCREF(o)
    py_obj.obj = <PyObject*>o
    py_obj.runtime = <PyObject*>self
    py_obj.type_flags = type_flags
    lua.luaL_getmetatable(L, POBJECT)
    lua.lua_setmetatable(L, -2)
    return 1 # values pushed


cdef lua.luaL_Reg py_object_lib[1]
py_object_lib[0] = lua.luaL_Reg(name="__call",
                                func=<lua.lua_CFunction> py_object_call)

cdef lua.luaL_Reg py_lib[7]
py_lib[0] = lua.luaL_Reg(name = "call",
                         func = <lua.lua_CFunction> py_as_attrgetter)
