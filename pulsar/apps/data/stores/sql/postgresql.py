from pulsar.utils.config import Global
from pulsar.apps.data import register_store, create_store

from .base import SqlStore, green_task


class PostgreSqlTestOption(Global):
    name = 'postgresql_server'
    flags = ['--postgresql-server']
    meta = "CONNECTION_STRING"
    default = 'postgresql://postgres@127.0.0.1:5432'
    desc = 'Default connection string for the PostgreSql server'


class PostgreSqlStore(SqlStore):

    @classmethod
    def register(cls):
        '''Postgresql requires the greenlet libraries and psycopg2
        '''
        from pulsar.apps.greenio import pg
        pg.make_asynchronous()

    @green_task
    def create_database(self, dbname=None, **kw):
        dbname = dbname or self.database
        store = create_store(self.dns, database='postgres', loop=self._loop)
        conn = store.sql_engine.connect()
        # when creating a database the connection must not be in a transaction
        conn.execute("commit")
        conn.execute("create database %s" % dbname)
        conn.close()
        return dbname

    @green_task
    def delete_database(self, dbname=None):
        # make sure no connections are opened
        self.sql_engine.dispose()
        dbname = dbname or self.database
        # switch to postgres database
        store = create_store(self.dns, database='postgres', loop=self._loop)
        conn = store.sql_engine.connect()
        conn.execute("commit")
        conn.execute("drop database %s" % dbname)
        conn.close()
        return dbname


register_store('postgresql', 'pulsar.apps.data.stores.PostgreSqlStore')
