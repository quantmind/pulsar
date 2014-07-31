from pulsar.utils.config import Global


class PostgreSqlOption(Global):
    name = 'postgresql_server'
    flags = ['--postgresql-server']
    meta = "CONNECTION_STRING"
    default = 'postgresql://postgres@127.0.0.1:5432'
    desc = 'Default connection string for the PostgreSql server'

try:
    from .base import SqlStore
except ImportError:     # pragma    nocover
    pass
else:
    from .postgresql import PostgreSqlStore
