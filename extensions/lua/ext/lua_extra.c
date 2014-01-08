#define LUA_LIB

#include "lua_extra.h"


#if PY_MAJOR_VERSION == 2
    STIN PyObject* native_str(const char* value) {
        return PyString_FromString(value);
    }
#else
    STIN PyObject* native_str(const char* value) {
        return PyUnicode_FromString(value);
    }
#endif

#define LUA_BASELIB ""

LUALIB_API PyObject* all_libs(lua_State *L) {
    PyObject *all = PyTuple_New(9);
    if (all) {
        PyTuple_SET_ITEM(all, 0, native_str(LUA_TABLIBNAME));
        PyTuple_SET_ITEM(all, 1, native_str(LUA_STRLIBNAME));
        PyTuple_SET_ITEM(all, 2, native_str(LUA_MATHLIBNAME));
        PyTuple_SET_ITEM(all, 3, native_str(LUA_DBLIBNAME));
        PyTuple_SET_ITEM(all, 4, native_str(LUA_COLIBNAME));
        PyTuple_SET_ITEM(all, 5, native_str(LUA_LOADLIBNAME));
        PyTuple_SET_ITEM(all, 6, native_str(LUA_OSLIBNAME));
        PyTuple_SET_ITEM(all, 7, native_str(LUA_IOLIBNAME));
        PyTuple_SET_ITEM(all, 8, native_str(CJSON_MODNAME));
    }
    return all;
}


LUALIB_API int load_lib(lua_State *L, const char* name) {
    if (strcmp(name, LUA_BASELIB)) {
        luaL_requiref(L, "_G", luaopen_base, 1);
    } else if (strcmp(name, LUA_TABLIBNAME)) {
        luaL_requiref(L, name, luaopen_table, 1);
    } else if (strcmp(name, LUA_STRLIBNAME)) {
        luaL_requiref(L, name, luaopen_string, 1);
    } else if (strcmp(name, LUA_MATHLIBNAME)) {
        luaL_requiref(L, name, luaopen_math, 1);
    } else if (strcmp(name, LUA_DBLIBNAME)) {
        luaL_requiref(L, name, luaopen_debug, 1);
    } else if (strcmp(name, LUA_COLIBNAME)) {
        luaL_requiref(L, name, luaopen_coroutine, 1);
    } else if (strcmp(name, LUA_LOADLIBNAME)) {
        luaL_requiref(L, name, luaopen_package, 1);
    } else if (strcmp(name, LUA_OSLIBNAME)) {
        luaL_requiref(L, name, luaopen_os, 1);
    } else if (strcmp(name, LUA_IOLIBNAME)) {
        luaL_requiref(L, name, luaopen_io, 1);
    } else if (strcmp(name, CJSON_MODNAME)) {
        luaopen_cjson(L);
    } else {
        return 0;
    }
    lua_pop(L, 1);
    return 1;
}
