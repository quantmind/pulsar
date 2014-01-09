.. _embedded-lua:

.. module:: pulsar.utils.lua

===================
Embedded Lua
===================

Lua_ is a lightweight embeddable scripting language. It is included in pulsar
mainly because of :ref:`pulsards <pulsar-data-store>`, the embedded data store
implemented along the lines of redis_.


Version
~~~~~~~~~~~~

    >>> from pulsar.utils.lua import Lua
    >>> l = Lua()
    >>> l.version()
    'Lua 5.2.2'


Execute
~~~~~~~~~~~~

    >>> from pulsar.utils.lua import Lua
    >>> l = Lua()
    >>> l.execute('return 1+6')
    7

Libraries
~~~~~~~~~~~~~~~
By default, when initialising lua, all standard libraries and the ``cjson``
extension are loaded into the lua state. Alternatively, one could prevent this
to happen by initialising as::

    >>> l = Lua(load_libs=False)
    >>> l.execute('return os.time()')
    Traceback (most recent call last):
    ...
    lua.LuaError: [string "<python>"]:1: attempt to index global 'os' (a nil value)

.. _Lua: http://www.lua.org/about.html
.. _redis: http://redis.io/
