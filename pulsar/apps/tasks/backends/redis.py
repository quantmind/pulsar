'''
A :class:`pulsar.apps.tasks.backends.TaskBackend` implementation
based on redis_ as data server.
Requires python-stdnet_ for mapping tasks into redis hashes.

.. _redis: http://redis.io/
.. _python-stdnet: https://pypi.python.org/pypi/python-stdnet
'''
from stdnet import odm

from pulsar import async
from pulsar.apps.tasks import backends, states
from pulsar.utils.log import local_method
from pulsar.utils.internet import get_connection_string


class TaskData(odm.StdModel):
    id = odm.SymbolField(primary_key=True)
    overlap_id = odm.SymbolField(required=False)
    name = odm.SymbolField()
    status = odm.SymbolField()
    args = odm.PickleObjectField()
    kwargs = odm.PickleObjectField()
    result = odm.PickleObjectField()
    from_task = odm.SymbolField(required=False)
    time_executed = odm.DateTimeField(index=False)
    time_started = odm.DateTimeField(required=False, index=False)
    time_ended = odm.DateTimeField(required=False, index=False)
    expiry = odm.DateTimeField(required=False, index=False)
    meta = odm.JSONField()
    #
    # List where all TaskData ids are queued
    queue = odm.ListField(class_field=True)
    # Set where TaskData ids under execution are stored
    executing = odm.SetField(class_field=True)
    
    class Meta:
        app_label = 'tasks'
        
    def as_task(self):
        params = dict(self.meta or {})
        for field in self._meta.scalarfields:
            params[field.name] = getattr(self, field.attname, None)
        return backends.Task(self.id, **params)
    
    def __unicode__(self):
        return '%s (%s)' % (self.name, self.status)


class TaskBackend(backends.TaskBackend):
    
    @classmethod
    def get_connection_string(cls, scheme, address, params, name):
        '''The ``name`` is used to set the ``namespace`` parameters in the
        connection string.''' 
        if name:
            params['namespace'] = '%s.' % name
        return get_connection_string(scheme, address, params)
            
    def num_tasks(self):
        '''Retrieve the number of tasks in the task queue.'''
        task_manager = self.task_manager()
        return task_manager.queue.size()
    
    @async()
    def put_task(self, task_id):
        if task_id:
            task_data = yield self._get_task(task_id)
            if task_data:
                task_data.status = states.QUEUED
                task_data = yield task_data.save()
                yield self.task_manager().queue.push_back(task_data.id)
                yield task_data.id
    
    @async()    
    def save_task(self, task_id, **params):
        # Called by self when the task need to be saved
        task_manager = self.task_manager()
        task_data = yield self._get_task(task_id)
        if task_data:
            for field, value in params.items():
                if field in task_data._meta.dfields:
                    setattr(task_data, field, value)
                else:
                    # not a field put value in the meta json field
                    task_data.meta[field] = value
            yield task_data.save()
        else:
            task_data = yield task_manager.new(id=task_id, **params)
        yield task_id
    
    def get_task(self, task_id=None, timeout=1):
        task_manager = self.task_manager()
        #
        if not task_id:
            task_id = yield task_manager.queue.block_pop_front(timeout=timeout)
        if task_id:
            task_data = yield self._get_task(task_id)
            if task_data:
                yield task_data.as_task()
        
    def get_tasks(self, **filters):
        task_manager = self.task_manager()
        tasks = yield task_manager.filter(**filters).all()
        yield [t.as_task() for t in tasks]
        
    def delete_tasks(self, ids=None):
        deleted = []
        if ids:
            task_manager = self.task_manager()
            tasks = yield task_manager.filter(id=ids).all()
            yield task_manager.filter(id=ids).delete()
            for task_data in tasks:
                deleted.append(task_data.id)
        yield deleted
        
    def flush(self):
        return self.models().flush()
        
    ############################################################################
    ##    INTERNALS
    @local_method
    def models(self):
        models = odm.Router(self.connection_string)
        models.register(TaskData)
        return models
        
    def task_manager(self):
        return self.models().taskdata

    def _get_task(self, task_id):
        tasks = yield self.task_manager().filter(id=task_id).all()
        if tasks:
            yield tasks[0]
            