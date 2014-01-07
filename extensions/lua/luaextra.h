#ifndef _LUAEXTRA_H_
#define _LUAEXTRA_H_

#include "lualib.h"


#ifdef _MSC_VER
#define INLINE __inline
#else
#define INLINE inline
#endif

INLINE int load_lib(lua_State *L, const char* name) {
    if (name == LUA_TABLIBNAME) {
        luaL_requiref(L, name, luaopen_table, 1);
    } else if (name == LUA_STRLIBNAME) {
        luaL_requiref(L, name, luaopen_string, 1);
    } else if (name == LUA_MATHLIBNAME) {
        luaL_requiref(L, name, luaopen_math, 1);
    } else if (name == LUA_DBLIBNAME) {
        luaL_requiref(L, name, luaopen_debug, 1);
    } else if (name == LUA_DBLIBNAME) {
        luaL_requiref(L, name, luaopen_debug, 1);
    } else {
        return 0;
    }
    return 1;
}


#endif  //  _LUAEXTRA_H_
