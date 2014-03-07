

class Database(object):
    '''Client for pulsar and redis stores.
    '''
    def __init__(self, store, name=None):
        self.store = store
        self.db = name or self.store._database

    def __repr__(self):
        return 'mongo.%s(%s)' % (self.db, self.store)
    __str__ = __repr__

    def execute(self, command, *args, **options):
        return self.store.execute(command, *args, **options)
    execute_command = execute
