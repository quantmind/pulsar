#ifndef _PULSAR_LUA_CONF_H_
#define _PULSAR_LUA_CONF_H_

#ifdef _MSC_VER
#include <stdlib.h>
#include <math.h>
#define STIN static __inline
#define EXIN extern __inline
#define snprintf _snprintf
#define isnan(x) _isnan(x)
#define isinf(x) (!_finite(x))
#define strncasecmp _strnicmp
#else
#define STIN static inline
#define EXIN extern inline
#endif

//#include <lua.h>

//#define LUA_CJSONLIBNAME   "cjson"
//LUAMOD_API int (luaopen_os) (lua_State *L);

#endif  //  _PULSAR_LUA_CONF_H_
