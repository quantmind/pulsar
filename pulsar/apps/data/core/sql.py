from . import client


class SqlDB(client.Store):
    '''Base :class:`.Store` class for SQL Databases.
    '''
    dbapi = None
    '''The dbapi module.'''
    field_overrides = {}
    for_update = False
    interpolation = '?'
    limit_max = None
    op_overrides = {}
    quote_char = '"'
    reserved_tables = []
    sequences = False

    def connect(self):
        '''Create a new connection with the database.'''
        raise NotImplementedError

    def compiler(self):
        return self.compiler_class(
            self.quote_char, self.interpolation, self.field_overrides,
            self.op_overrides)

    def create_table(self, model_class):
        qc = self.compiler()
        return self.execute(qc.create_table(model_class))
