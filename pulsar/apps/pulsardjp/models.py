'''\
Models for traking tasks and servers.
Requires python-stdnet_

.. _python-stdnet: http://pypi.python.org/pypi/python-stdnet/
'''
import platform

try:
    from stdnet import orm
    from stdnet.utils import to_string
    
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


    class Task(orm.StdModel):
        filtering = ('id','name','status','user')
        id = orm.SymbolField(primary_key = True)
        name = orm.SymbolField()
        status = orm.SymbolField()
        time_executed = orm.DateTimeField()
        time_start = orm.DateTimeField(required = False)
        time_end = orm.DateTimeField(required = False)
        result = orm.PickleObjectField(required = False)
        user = orm.SymbolField(required = False)
        api = orm.SymbolField(required = False)
    
    
except ImportError:
    PulsarServer = None
    Task = None

