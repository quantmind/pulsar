from pulsar.apps.data import odm
from pulsar.apps.tasks import Task

models = odm.Mapper('couchdb://127.0.0.1:5984/test')
store = models.default_store
try:
    ok = store.delete_database()
except Exception:
    pass
ok = store.create_database()

models.register(Task)
ok = models.create_tables()

tasks = models.task
task = tasks.create(name='bla')
assert task.name == 'bla'
task = tasks.create(name='foo')
assert task.name == 'foo'

objs = store.table_info(Task)
ok = models.drop_tables()
print(objs)
