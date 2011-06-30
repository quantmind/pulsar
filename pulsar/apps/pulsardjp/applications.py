from datetime import datetime

from djpcms import forms, views, html
from djpcms.template import loader
from djpcms.apps.included.admin import AdminApplication, AdminApplicationSimple
from djpcms.utils.dates import nicetimedelta
from djpcms.utils.text import nicename
from djpcms.utils import mark_safe
from djpcms.forms.utils import return_form_errors

from stdnet import orm

import pulsar
from pulsar.utils.py2py3 import iteritems
from pulsar.apps import tasks
from pulsar.http import rpc

from .models import Task

fromtimestamp = datetime.fromtimestamp


class ServerForm(forms.Form):
    code = forms.CharField()
    schema = forms.CharField(initial = 'http://')
    host = forms.CharField()
    port = forms.IntegerField(initial = 8060)
    notes = forms.CharField(widget = html.TextArea,
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
                             'value':html.ObjectDefinition(self,djp,\
                                          self.pannel_data(monitor))})
        servers = [{'name':'Server',
                    'value':html.ObjectDefinition(self,djp,\
                                   self.pannel_data(info['server']))}]
        return {'left_panels':servers,
                'right_panels':monitors}


task_display = ('name','status','timeout','time_executed',
                'time_start','time_end','duration','expiry',
                'user')

class TasksAdmin(AdminApplicationSimple):
    list_display = ('short_id',) + task_display
    object_display = ('id',) + task_display + ('api','string_result','stack_trace') 
    has_plugins = False
    inherit = False
    search = views.SearchView()
    view   = views.ViewView(regex = views.UUID_REGEX)
    delete = views.DeleteView()
    

#
# Scripts
#

script_languages = (
                    ('python','python'),
                    )


class ScriptForm(forms.Form):
    name = forms.CharField(toslug = '_')
    language = forms.ChoiceField(choices = script_languages)
    body = forms.CharField(widget = html.TextArea(default_class = 'taboverride'))
    
    def clean_name(self, value):
        return orm.test_unique('name',self.model,value,self.instance,
                               forms.ValidationError)
    

class RunScriptForm(forms.Form):
    parameters = forms.CharField(required = False)
    
    def clean_parameters(self, value):
        return value
    

class RunScriptView(views.ChangeView):
    
    def default_post(self, djp):
        fhtml = self.get_form(djp)
        form = fhtml.form
        if form.is_valid():
            self.appmodel.run(djp)
        else:
            return return_form_errors(fhtml,djp)
    

HtmlRunScriptForm = forms.HtmlForm(
    RunScriptForm,
    #layout = Layout(default_style = blockLabels2),
    inputs = (('run','_save'),)
)
    
    
class ScriptApplication(views.ModelApplication):
    inherit = True
    form = ScriptForm
    list_display = ('name','language','parameters')
    
    run_view = RunScriptView(regex = 'run', form = HtmlRunScriptForm)
    
    class Media:
        js = ['djpcms/taboverride.js']
    
    def run(self, djp, **params):
        '''This needs to be implemented by your application'''
        pass
        
        
