from pulsar.utils.security import gen_unique_id
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
task1 = tasks.create(name='bla', id=gen_unique_id())
assert task1.name == 'bla'
task2 = tasks.create(name='foo', id=gen_unique_id())
assert task2.name == 'foo'
task3 = tasks.create(name='foo', id=gen_unique_id())
assert task3.name == 'foo'

objs = store.table_info(Task)

store.

ok = models.drop_tables()
