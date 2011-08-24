'''\
Models for tracking tasks and servers.
Requires python-stdnet_

.. _python-stdnet: http://pypi.python.org/pypi/python-stdnet/
'''
import platform

from pulsar.apps import tasks


try:
    from stdnet import orm
    from stdnet.utils import to_string
    from djpcms.utils.text import nicename
    
    class PulsarServer(orm.StdModel):
        code = orm.SymbolField()
        host = orm.SymbolField()
        port = orm.IntegerField(default = 8060, index = False)
        notes = orm.CharField(required = False)
        location = orm.CharField()
        schema = orm.CharField(default = 'http://')
        
        def __unicode__(self):
            return to_string('{0} - {1}'.format(self.code,self.path()))
        
        def this(self):
            return self.host == platform.node()
        this.boolean = True
        
        def path(self):
            ap = ''
            if self.port:
                ap = ':%s' % self.port
            return '{0}{1}{2}'.format(self.schema,self.host,ap)


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
        timeout = orm.BooleanField(default = False)
        args = orm.PickleObjectField()
        kwargs = orm.PickleObjectField()
        result = orm.PickleObjectField(required = False)
        stack_trace = orm.CharField()
        
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
        
        def tojson_dict(self):
            d = self.todict()
            d['id'] = self.id
            return d
        
        def _on_finish(self, worker):
            duration = self.duration()
            if duration:
                self.task_duration = 86400*duration.days + duration.seconds
                self.save()
        
        @classmethod
        def get_task(cls, id, remove = True):
            try:
                task = cls.objects.get(id = id)
                if remove and task.done():
                    task.delete()
                else:
                    return task
            except cls.DoesNotExist:
                pass
    
    
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

