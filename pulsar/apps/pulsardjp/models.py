'''\
Models for tracking tasks and servers.
Requires python-stdnet_

.. _python-stdnet: http://pypi.python.org/pypi/python-stdnet/
'''
import platform

from pulsar.apps import tasks
from pulsar.utils.timeutils import timedelta_seconds


try:
    from stdnet import orm
    from stdnet.utils import to_string
    from stdnet.apps import grid
    from djpcms.utils.text import nicename
    
    class PulsarServer(orm.StdModel):
        code = orm.SymbolField()
        path = orm.CharField(required = True)
        notes = orm.CharField(required = False)
        location = orm.CharField()
        
        def __unicode__(self):
            return to_string('{0} - {1}'.format(self.code,self.path))
        
        def this(self):
            return False
        this.boolean = True

    class JobModel(orm.FakeModel):
        
        def __init__(self, name, data = None, proxy = None):
            self.proxy = proxy
            if data:
                for head in data:
                    setattr(self,head,data[head])
            self.id = name
            self.name = nicename(name)
            
        def __unicode__(self):
            return self.name
        
        def tasks(self):
            return Task.objects.filter(name = self.id)


    class Task(orm.StdModel, tasks.Task):
        '''A Task Stored in Redis'''
        filtering = ('id','name','status','user')
        id = orm.SymbolField(primary_key = True)
        name = orm.SymbolField()
        status = orm.SymbolField()
        user = orm.SymbolField(required = False)
        api = orm.SymbolField(required = False)
        
        time_executed = orm.DateTimeField(index = False)
        time_start = orm.DateTimeField(required = False, index = False)
        time_end = orm.DateTimeField(required = False, index = False)
        task_duration = orm.FloatField(required = False)
        expiry = orm.DateTimeField(required = False, index = False)
        timeout = orm.DateTimeField(required = False)
        args = orm.PickleObjectField()
        kwargs = orm.PickleObjectField()
        result = orm.PickleObjectField(required = False)
        logs = orm.CharField()
        
        class Meta:
            ordering = '-time_executed'
        
        def short_id(self):
            return self.id[:8]
        
        @property
        def job(self):
            return JobModel(self.name)
        
        def __unicode__(self):
            return '{0}({1})'.format(self.name,self.id[:8])
        
        def round(self,v):
            return 0.001*int(1000*v.seconds)
        
        def string_result(self):
            return str(self.result)
        string_result.short_description = 'result'
        
        def on_created(self, worker = None):
            self.save()
            
        def on_received(self, worker = None):
            self.save()
        
        def on_start(self, worker = None):
            self.save()
    
        def on_finish(self, worker):
            duration = self.duration()
            if duration:
                self.task_duration = timedelta_seconds(duration)
                self.save()
            return self
        
        @classmethod
        def get_task(cls, id, remove = False):
            try:
                task = cls.objects.get(id = id)
                if task.done() and remove:
                    task.delete()
                    task.id = tasks.create_task_id()
                    task.save()
                else:
                    return task
            except cls.DoesNotExist:
                pass
    
    
    class Queue(grid.Queue):
        queue = orm.ListField(Task)
        
    
    class Script(orm.StdModel):
        name = orm.SymbolField(unique = True)
        body = orm.CharField()
        language = orm.SymbolField()
        
        def __unicode__(self):
            return self.name
        
except ImportError:
    PulsarServer = None
    Task = None
    Script = None

