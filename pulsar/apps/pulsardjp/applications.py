from datetime import datetime

from djpcms.template import loader
from djpcms.apps.included.admin import AdminApplication, AdminApplicationSimple
from djpcms.html import LazyRender, Table, ObjectDefinition
from djpcms.utils.dates import nicetimedelta
from djpcms.utils.text import nicename
from djpcms.utils import mark_safe
from djpcms import forms, views

import pulsar
from pulsar.utils.py2py3 import iteritems
from pulsar.apps.tasks import states, consumer, TaskException, get_traceback
from pulsar.http import rpc

from .models import Task

fromtimestamp = datetime.fromtimestamp

class ServerForm(forms.Form):
    code = forms.CharField()
    schema = forms.CharField(initial = 'http://')
    host = forms.CharField()
    port = forms.IntegerField(initial = 8060)
    notes = forms.CharField(widget = forms.TextArea,
                            required = False)
    location = forms.CharField(required = False)
    

class PulsarServerApplication(AdminApplication):
    inherit = True
    form = ServerForm
    list_per_page = 100
    template_view = ('pulsardjp/monitor.html',)
    converters = {'uptime': nicetimedelta}
    list_display = ('code','path','machine','this','notes')
     
    def get_client(self, instance):
        return rpc.JsonProxy(instance.path())
        
    def render_object_view(self, djp):
        r = self.get_client(djp.instance)
        try:
            panels = self.get_panels(djp,r.server_info())
        except pulsar.ConnectionError:
            panels = {'left_panels':[{'name':'Server','value':'No Connection'}]}
        return loader.render(self.template_view,panels)
    
    def pannel_data(self, data):
        for k,v in iteritems(data):
            if k in self.converters:
                v = self.converters[k](v)
            yield {'name':nicename(k),
                   'value':v}
            
    def get_panels(self,djp,info):
        monitors = []
        for monitor in info['monitors']:
            monitors.append({'name':nicename(monitor.pop('name','Monitor')),
                             'value':ObjectDefinition(self,djp,\
                                          self.pannel_data(monitor))})
        servers = [{'name':'Server',
                    'value':ObjectDefinition(self,djp,\
                                   self.pannel_data(info['server']))}]
        return {'left_panels':servers,
                'right_panels':monitors}


task_display = ('id','name','status','time_executed',
                'time_start','time_end','duration',
                'user')

class TasksAdmin(AdminApplicationSimple):
    list_display = task_display
    object_display = list_display + ('api','string_result','stack_trace') 
    has_plugins = False
    inherit = False
    search = views.SearchView()
    view   = views.ViewView(regex = views.UUID_REGEX)
    delete = views.DeleteView()
    
    
class TaskRequest(consumer.TaskRequest):
    
    def _on_init(self):
        if self.ack:
            Task(id = self.id,
                 name = self.name,
                 time_executed = fromtimestamp(self.time_executed),
                 status = states.PENDING).save()
             
    def _on_start(self,worker):
        if self.ack:
            t = Task.objects.get(id = self.id)
            t.status = states.STARTED
            t.time_start = fromtimestamp(self.time_start)
            t.save()
            return t
        
    def _on_finish(self,worker):
        if self.ack:
            t = Task.objects.get(id = self.id)
            if self.exception:
                t.status = states.FAILURE
                if isinstance(self.exception,TaskException):
                    t.stack_trace = self.exception.stack_trace
                else:
                    t.stack_trace = get_traceback()
                t.result = str(self.exception)
            else:
                t.status = states.SUCCESS
                t.result = self.result
            t.time_end = fromtimestamp(self.time_end)
            t.save()
    
    def todict(self):
        try:
            task = Task.objects.get(id = self.id)
        except Task.DoesNotExist:
            return super(TaskRequest,self).todict()
        d = task.todict()
        d['id'] = task.id
        return d
        
    @classmethod
    def get_task(cls, id, remove = False):
        try:
            task = Task.objects.get(id = id)
        except Task.DoesNotExist:
            return None
        if remove and task.time_end:
            task.delete()
        return task.todict()
    
