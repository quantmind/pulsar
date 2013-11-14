from sqlalchemy.engine import Connection
from sqlalchemy.sql import ddl
from sqlalchemy.sql.ddl import CreateIndex, CreateTable, CreateSequence

from pulsar import in_loop, coroutine_return
from pulsar.utils.pep import is_string

from .manager import Manager
from . import client


class SqlConnection(Connection):

    def __init__(self, store, connection, **kw):
        super(SqlConnection, self).__init__(store, connection, **kw)
        self._loop = store._loop
        self._sock_fd = connection.fileno()

    @property
    def connection(self):
        return self._Connection__connection

    @property
    def transaction(self):
        return self._Connection__transaction

    @property
    def info(self):
        """Info dictionary associated with the underlying DBAPI connection
        referred to by this :class:`.SqlConnection`, allowing user-defined
        data to be associated with the connection.

        The data here will follow along with the DBAPI connection including
        after it is returned to the connection pool and used again
        in subsequent instances of :class:`.SqlConnection`.

        """
        return self.connection.info

    def close(self, exc=None):
        """Closes the transport.

        Buffered data will be flushed asynchronously.  No more data
        will be received.  After all buffered data is flushed, the
        :class:`BaseProtocol.connection_lost` method will (eventually) called
        with ``None`` as its argument.
        """
        connection = self.connection
        if connection is not None:
            self._connection = None
            try:
                connection.close()
            except Exception:
                pass
            self._loop.remove_connector(self._sock_fd)
            self._sock_fd = None

    def cursor(self, **kw):
        return self.connection.cursor(**kw)

    def _execute_context(self, dialect, constructor,
                                    statement, parameters,
                                    *args):
        """Create an :class:`.ExecutionContext` and execute, returning
        a :class:`.ResultProxy`."""
        context = constructor(dialect, self, self.connection, *args)
        if context.compiled:
            context.pre_exec()

        cursor, statement, parameters = context.cursor, \
                                        context.statement, \
                                        context.parameters

        if not context.executemany:
            parameters = parameters[0]

        if self._has_events:
            for fn in self.dispatch.before_cursor_execute:
                statement, parameters = \
                            fn(self, cursor, statement, parameters,
                                        context, context.executemany)

        if context.executemany:
            self.dialect.do_executemany(
                                cursor,
                                statement,
                                parameters,
                                context)
        elif not parameters and context.no_parameters:
            self.dialect.do_execute_no_params(
                                cursor,
                                statement,
                                context)
        else:
            self.dialect.do_execute(
                                cursor,
                                statement,
                                parameters,
                                context)
        yield cursor.waiting

        if self._has_events:
            self.dispatch.after_cursor_execute(self, cursor,
                                                statement,
                                                parameters,
                                                context,
                                                context.executemany)

        if context.compiled:
            context.post_exec()

            if context.isinsert and not context.executemany:
                context.post_insert()

        # create a resultproxy, get rowcount/implicit RETURNING
        # rows, close cursor if no further results pending
        result = context.get_result_proxy()
        if context.isinsert:
            if context._is_implicit_returning:
                context._fetch_implicit_returning(result)
                result.close(_autoclose_connection=False)
                result._metadata = None
            elif not context._is_explicit_returning:
                result.close(_autoclose_connection=False)
                result._metadata = None
        elif context.isupdate and context._is_implicit_returning:
            context._fetch_implicit_update_returning(result)
            result.close(_autoclose_connection=False)
            result._metadata = None

        elif result._metadata is None:
            # no results, get rowcount
            # (which requires open cursor on some drivers
            # such as kintersbasdb, mxodbc),
            result.rowcount
            result.close(_autoclose_connection=False)

        #if self.transaction is None and context.should_autocommit:
        #    self._commit_impl(autocommit=True)

        if result.closed and self.should_close_with_result:
            self.close()

        return result


class SchemaGenerator(ddl.SchemaGenerator):

    def visit_table(self, table, create_ok=False):
        if not create_ok and not self._can_create_table(table):
            coroutine_return()
        for column in table.columns:
            if column.default is not None:
                yield self.traverse_single(column.default)
        yield self.connection.execute(CreateTable(table))
        for index in getattr(table, 'indexes', ()):
            yield self.traverse_single(index)

    def visit_sequence(self, sequence, create_ok=False):
        if not create_ok and not self._can_create_sequence(sequence):
            return
        return self.connection.execute(CreateSequence(sequence))

    def visit_index(self, index):
        return self.connection.execute(CreateIndex(index))


class SqlManager(Manager):

    def __call__(self, **kw):
        ins = self._model.insert().values(**kw)

    def insert(self, **kw):
        ins = self._model.insert().values(**kw)
        return self._store.execute(ins)


class SqlDB(client.Store):
    '''Base :class:`.Store` class for SQL Databases.
    '''
    dbapi = None
    '''The dbapi module.'''
    dialect = None
    default_manager = SqlManager
    _execution_options = {}
    _has_events = False

    def connect(self):
        '''Create a new connection with the database.'''
        raise NotImplementedError

    def raw_connection(self):
        '''raw connection to db.'''
        raise NotImplementedError

    @in_loop
    def execute(self, statement, *multiparams, **params):
        conn = yield self._pool.connect()
        with conn:
            result = yield conn.execute(statement, *multiparams, **params)
            coroutine_return(result)

    @in_loop
    def create_table(self, table, connection=None, **kw):
        if connection is None:
            connection = yield self._pool.connect()
        with connection:
            gen = SchemaGenerator(self.dialect, connection, **kw)
            result = yield gen.traverse_single(table)

    def compile_query(self, query):
        pass

    # SQLALCHEMY ENGINE UNUSED
    def _should_log_info(self):
        return False
