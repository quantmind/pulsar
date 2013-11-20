
cdef extern from "lua.h" nogil:
    char* LUA_RELEASE
    int LUA_MULTRET

    int LUA_REGISTRYINDEX

    enum:
        LUA_YIELD      # 1
        LUA_ERRRUN     # 2
        LUA_ERRSYNTAX  # 3
        LUA_ERRMEM     # 4
        LUA_ERRERR     # 5

    enum:
        LUA_TNONE             # -1
        LUA_TNIL              # 0
        LUA_TBOOLEAN          # 1
        LUA_TLIGHTUSERDATA    # 2
        LUA_TNUMBER           # 3
        LUA_TSTRING           # 4
        LUA_TTABLE            # 5
        LUA_TFUNCTION         # 6
        LUA_TUSERDATA         # 7
        LUA_TTHREAD           # 8

    ctypedef struct lua_State
    ctypedef int (*lua_CFunction) (lua_State *L)


    void  lua_close (lua_State *L)
    void  lua_newtable (lua_State *L)
    void  lua_settable (lua_State *L, int index)
    void lua_pushcfunction (lua_State *L, lua_CFunction f)
    const char *lua_pushstring (lua_State *L, const char *s)
    void lua_setglobal (lua_State *L, const char *name)

    # iteration
    int   lua_next (lua_State *L, int idx)

    # basic stack manipulation
    int   lua_gettop (lua_State *L)
    void  lua_settop (lua_State *L, int idx)
    void  lua_pushvalue (lua_State *L, int idx)
    void  lua_remove (lua_State *L, int idx)
    void  lua_insert (lua_State *L, int idx)
    void  lua_replace (lua_State *L, int idx)
    int   lua_checkstack (lua_State *L, int sz)

    # access functions (stack -> C)
    int     lua_type (lua_State *L, int idx)
    float   lua_tonumber (lua_State *L, int idx)
    bint    lua_toboolean (lua_State *L, int idx)
    char   *lua_tolstring (lua_State *L, int idx, size_t *len)

    # push functions (C -> stack)
    void  lua_pushnil (lua_State *L)
    void  lua_pushboolean (lua_State *L, int b)
    void  lua_pushnumber (lua_State *L, float n)
    const char *lua_pushlstring (lua_State *L, const char *s, size_t len)

    # get/set Lua/stack functions
    void  lua_rawgeti (lua_State *L, int idx, int n)
    void  lua_rawseti (lua_State *L, int idx, int n)

    # `load' and `call' functions (load and run Lua code)
    void  lua_call (lua_State *L, int nargs, int nresults)
    int   lua_pcall (lua_State *L, int nargs, int nresults, int errfunc)

    # useful macros
    void lua_pop(lua_State *L, int idx)
    bint lua_istable(lua_State *L, int n)
    bint lua_isnil(lua_State *L, int n)
    bint lua_isnone(lua_State *L,int n)


cdef extern from "lauxlib.h" nogil:

    int luaL_loadbuffer (lua_State *L, char *buff, size_t sz, char *name)
    int luaL_ref (lua_State *L, int t)
    void luaL_unref (lua_State *L, int t, int ref)


cdef extern from "lualib.h":

    lua_State *luaL_newstate ()
    void luaL_openlibs(lua_State *L)
    void luaL_requiref (lua_State *L, const char *modname,
                        lua_CFunction openf, int glb)

