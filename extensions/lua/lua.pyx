# This file is part of pulsar released under the BSD license.
# See the LICENSE for more information.
cimport lua
from lua cimport lua_State

cimport cpython
cimport cpython.float
cimport cpython.long
cimport cpython.ref
cimport cpython.tuple
from cpython.version cimport PY_MAJOR_VERSION

from threading import RLock
from collections import Mapping, Iterable

cdef extern from *:
    ctypedef char* const_char_ptr "const char*"


if PY_MAJOR_VERSION < 3:
    string_type = unicode
    int_type = (int, long)
else:
    string_type = str
    int_type = long


cdef inline bytes to_bytes(object value, str encoding):
    if isinstance(value, bytes):
        return value
    elif isinstance(value, string_type):
        return value.encode(encoding)
    else:
        raise TypeError('Requires bytes or string')


cdef inline object native_str(object value):
    if PY_MAJOR_VERSION < 3:
        if isinstance(value, unicode):
            return value.encode('utf-8')
        else:
            return value
    else:
        if isinstance(value, bytes):
            return value.decode('utf-8')
        else:
            return value


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

    :param load_libs: load all standard libraries by default.
    :param convert: if true it convert lua tables into python lists/dicts
        otherwise it return a LuaObject. Default False.
    :param charset: encoding charset.
    """
    cdef lua_State *state
    cdef tuple raised_exception
    cdef str charset
    cdef str encoding
    cdef bint convert
    cdef object lock
    cdef dict scripts
    cdef dict _libs

    def __cinit__(self, encoding=None, load_libs=True, charset=None):
        self.state = lua.luaL_newstate()
        if self.state is NULL:
            raise LuaError("Failed to initialise Lua runtime")
        self._libs = {}
        self.scripts = {}
        self.charset = charset or 'utf-8'
        self.encoding = encoding
        self.convert = True
        self.lock = RLock()
        if load_libs:
            lua.luaL_openlibs(self.state)
            lua.lua_settop(self.state, 0)

    def __dealloc__(self):
        if self.state is not NULL:
            lua.lua_close(self.state)
            self.state = NULL

    def version(self):
        '''Lua version number'''
        return native_str(lua.LUA_RELEASE)

    def execute(self, lua_code):
        """Execute a Lua script passed in a string.
        """
        return self._run_lua(to_bytes(lua_code, self.charset))

    def loadlib(self, libname):
        cdef bytes name = to_bytes(libname, self.charset)
        return lua.load_lib(self.state, name)

    def register(self, str lib_name, object handler, *methods):
        '''Register a python object as a lua table.'''
        cdef bytes lname
        cdef dict callables = {}
        if not methods:
            return
        with self:
            if lib_name in self._libs:
                raise KeyError('Lib %s already registered' % lib_name)
            lua.lua_newtable(self.state)
            for method in methods:
                if method not in callables:
                    callables[method] = self._add_method(handler, method)
            lname = to_bytes(lib_name, self.charset)
            lua.lua_setglobal(self.state, lname)
            # increase the reference count of the handler
            self._libs[lib_name] = (handler, callables)
            return True

    def openlibs(self):
        lua.luaL_openlibs(self.state)

    def set_global(self, name, value):
        '''Set a global variable

        ``value`` must be a number, a Mapping or an Iterable
        '''
        cdef bytes bname
        if _py_to_lua(self.state, value):
            bname = to_bytes(name, 'utf-8')
            lua.lua_setglobal(self.state, bname)
        else:
            raise RuntimeError('Could not set global variable %s to %s' %
                               (name, value))

    def __enter__(self):
        self.lock.acquire()

    def __exit__(self, *args):
        lua.lua_settop(self.state, 0)
        self.lock.release()

    cdef object _add_method(self, handler, method):
        cdef bytes name = to_bytes(method, self.charset)
        cdef object callable = getattr(handler, method, None)
        if not hasattr(callable, '__call__'):
            raise TypeError('%s is an invalid callable for %s' %
                            (method, handler))
        lua.lua_pushlstring(self.state, name, len(name))
        lua.lua_pushlightuserdata(self.state, <void*>callable)
        lua.lua_pushcclosure(self.state, <lua.lua_CFunction>py_call, 1)
        lua.lua_settable(self.state, -3)
        return callable

    cdef inline object _run_lua(self, bytes lua_code):
        cdef int result, nargs
        with self:
            if lua.luaL_loadbuffer(self.state,
                                   lua_code,
                                   len(lua_code),
                                   '<python>'):
                raise LuaSyntaxError(
                    _lua_error_message(self.state,
                                       "error loading code: %s", -1))
            with nogil:
                result = lua.lua_pcall(self.state, 0, lua.LUA_MULTRET, 0)
            if result:
                _raise_lua_error(self.state, result)
            nargs = lua.lua_gettop(self.state)
            if nargs == 1:
                return _py_from_lua(self.state, 1)
            elif nargs:
                return _unpack(self.state)
            else:
                return None


##    INTERNALS
cdef int py_call(lua_State *state) nogil:
    return py_call_with_gil(state)


cdef int py_call_with_gil(lua_State* state) with gil:
    cdef const void* ptr = lua.lua_topointer(state, lua.lua_upvalueindex(1))
    cdef object method = <object>ptr
    cdef tuple args = _unpack(state)
    return _py_to_lua(state, method(*args))


cdef str _lua_error_message(lua_State *state, str err_message, int n):
    cdef size_t size = 0
    cdef const_char_ptr s = lua.lua_tolstring(state, n, &size)
    cdef string = s[:size].decode('utf-8')
    return err_message % string if err_message else string


cdef _raise_lua_error(lua_State *state, int result):
    if result == lua.LUA_ERRMEM:
        cpython.exc.PyErr_NoMemory()
    else:
        raise LuaError(_lua_error_message(state, None, -1))


cdef inline tuple _unpack(lua_State *state, Lua runtime=None):
    cdef int nargs = lua.lua_gettop(state)
    cdef tuple args = cpython.tuple.PyTuple_New(nargs)
    cdef int i
    for i in range(nargs):
        arg = _py_from_lua(state, i+1)
        cpython.ref.Py_INCREF(arg)
        cpython.tuple.PyTuple_SET_ITEM(args, i, arg)
    return args


cdef inline object _py_from_lua(lua_State *state, int n, Lua runtime=None):
    cdef size_t size = 0
    cdef const_char_ptr s
    cdef float number
    cdef int lua_type = lua.lua_type(state, n)

    if lua_type == lua.LUA_TNIL:
        return None
    elif lua_type == lua.LUA_TNUMBER:
        number = lua.lua_tonumber(state, n)
        if number != <int>number:
            return <double>number
        else:
            return <int>number
    elif lua_type == lua.LUA_TSTRING:
        s = lua.lua_tolstring(state, n, &size)
        return s[:size]
    elif lua_type == lua.LUA_TBOOLEAN:
        return lua.lua_toboolean(state, n)
    elif lua_type == lua.LUA_TTABLE:
        return _py_from_lua_table(state, n, runtime)
    else:
        raise LuaError('Lua return type not supported')


cdef object _py_from_lua_table(lua_State *state, int n, Lua runtime=None):
    cdef object key, val
    cdef int lua_type, index = 0
    cdef bint as_list = True
    keys = []
    vals = []
    # push the table at the top of the stack
    lua.lua_pushvalue(state, n)
    try:
        # push first (dummy) key into the stack
        lua.lua_pushnil(state)
        while lua.lua_next(state, -2):
            index += 1
            try:
                key = _py_from_lua(state, -2, runtime)
                val = _py_from_lua(state, -1, runtime)
                keys.append(key)
                vals.append(val)
                as_list = as_list and key == index
            finally:
                # pop the last value from the stack
                lua.lua_pop(state, 1)
        if as_list:
            return vals
        else:
            return dict(zip(keys, vals))
    finally:
        # remove the table from the stack
        lua.lua_pop(state, 1)


cdef inline int _py_to_lua(lua_State *state, object o, Lua runtime=None):
    '''called to convert a python object when a python callable is
    called in lua'''
    cdef int n
    cdef bytes b
    if type(o) is bool:
        lua.lua_pushboolean(state, <bint>o)
    elif type(o) is float:
        lua.lua_pushnumber(state, <float>cpython.float.PyFloat_AS_DOUBLE(o))
    elif isinstance(o, int_type):
        lua.lua_pushnumber(state, <float>o)
    elif isinstance(o, bytes):
        b = <bytes>o
        lua.lua_pushlstring(state, b, len(b))
    elif ((PY_MAJOR_VERSION < 3 and isinstance(o, unicode)) or
          (PY_MAJOR_VERSION > 2 and isinstance(o, str))):
        b = o.encode('utf-8')
        lua.lua_pushlstring(state, b, len(b))
    elif runtime is None:
        if isinstance(o, Mapping):
            lua.lua_newtable(state);
            for key in o:
                if _py_to_lua(state, key):
                    if _py_to_lua(state, o[key]):
                        lua.lua_settable(state, -3)
                    else:
                        lua.lua_pop(state, 1)
        elif isinstance(o, Iterable):
            lua.lua_newtable(state);
            for n, value in enumerate(o, 1):
                lua.lua_pushnumber(state, <float>n)
                if _py_to_lua(state, value):
                    lua.lua_settable(state, -3)
                else:
                    lua.lua_pop(state, 1)
        else:
            return 0
    else:
        return 0
    return 1
