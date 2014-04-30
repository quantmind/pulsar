from sqlalchemy.engine import create_engine
from sqlalchemy.ext.declarative import declarative_base, DeclarativeMeta
from sqlalchemy.orm import sessionmaker

from pulsar.apps.data import Store, register_store
from pulsar.apps.greenio import green_task
from pulsar.utils.pep import itervalues


Base = declarative_base()


class SqlStore(Store):
    '''A pulsar :class:`.Store` based on sqlalchemy
    '''
    def _init(self, **kwargs):
        dns = self._buildurl()
        self.sql_engine = create_engine(dns, **kwargs)
        self.transaction = sessionmaker(bind=self.sql_engine)

    @green_task
    def ping(self):
        conn = self.sql_engine.connect()
        r = conn.execute('select 1;')
        result = r.fetchone()
        return result[0] == 1

    @green_task
    def create_table(self, model, remove_existing=False):
        sql_model = self.sql_model(model)
        table = sql_model.__table__
        return table.create(self.sql_engine, checkfirst=remove_existing)

    @green_task
    def drop_table(self, model, remove_existing=False):
        sql_model = self.sql_model(model)
        table = sql_model.__table__
        return table.drop(self.sql_engine, checkfirst=remove_existing)

    def close(self):
        # make sure no connections are opened
        self.sql_engine.dispose()

    # ODM SUPPORT

    def create_model(self, manager, *args, **kwargs):
        sql_model = self.sql_model(manager._model)
        return sql_model(*args, **kwargs)

    @green_task
    def execute_transaction(self, transaction):
        '''Execute a ``transaction``
        '''
        transaction.commit()
        return list(itervalues(transaction.identity_map))
    # INTERNALS

    def sql_model(self, model):
        sql_model = getattr(model, '_sql_model', None)
        if not sql_model:
            meta = model._meta
            fields = {'__tablename__': meta.table_name}
            for field in model._meta.dfields.values():
                fields[field.store_name] = field.sql_alchemy_column()
            sql_model = DeclarativeMeta(meta.name, (Base,), fields)
            model._sql_model = sql_model
            sql_model._meta = meta
        return sql_model
