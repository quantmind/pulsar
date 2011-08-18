from datetime import datetime

import djpcms
from djpcms import forms, views, html, ajax
from djpcms.template import loader
from djpcms.apps.included.admin import AdminApplication,\
                                        AdminApplicationSimple,\
                                        TabView
from djpcms.utils.dates import nicetimedelta, smart_time
from djpcms.utils.text import nicename
from djpcms.utils import mark_safe
from djpcms.forms.utils import return_form_errors

from stdnet import orm

import pulsar
from pulsar.utils.py2py3 import iteritems
from pulsar.apps import tasks
from pulsar.http import rpc

from .models import Task, JobModel

fromtimestamp = datetime.fromtimestamp

monitor_template = '''\
<div class="yui-g">
    <div class="yui-u first">
        <div class="pulsar-panel">{% for panel in left_panels %}
         <div class="flat-panel">
          <div class="hd">
           <h2>{{ panel.name }}</h2>
          </div>
          <div class="bd">
           {{ panel.value }}
          </div>
         </div>{% endfor %}
        </div>
    </div>
    <div class="yui-u"> 
        <div class="pulsar-panel">{% for panel in right_panels %}
         <div class="flat-panel">
          <div class="hd">
           <h2>{{ panel.name }}</h2>
          </div>
          <div class="bd">
           {{ panel.value }}
          </div>
         </div>{% endfor %}
        </div>
    </div>
</div>'''


class ServerForm(forms.Form):
    code = forms.CharField()
    schema = forms.CharField(initial = 'http://')
    host = forms.CharField()
    port = forms.IntegerField(initial = 8060)
    notes = forms.CharField(widget = html.TextArea,
                            required = False)
    location = forms.CharField(required = False)
    

class ServerView(TabView):
    converters = {'uptime': nicetimedelta,
                  'notified': smart_time,
                  'default_timeout': nicetimedelta,
                  'timeout': nicetimedelta}
    
    def get_client(self, instance):
        return rpc.JsonProxy(instance.path())
        
    def render_object_view(self, djp, appmodel, instance):
        r = self.get_client(instance)
        try:
            panels = self.get_panels(djp,appmodel,instance,r.server_info())
        except pulsar.ConnectionError:
            panels = {'left_panels':
                      [{'name':'Server','value':'No Connection'}]}
        return loader.template_class(monitor_template).render(panels)
    
    def pannel_data(self, data):
        for k,v in iteritems(data):
            if k in self.converters:
                v = self.converters[k](v)
            yield {'name':nicename(k),
                   'value':v}
            
    def get_panels(self,djp,appmodel,instance,info):
        monitors = []
        for monitor in info['monitors']:
            workers = monitor.pop('workers',None)
            monitors.append({'name':nicename(monitor.pop('name','Monitor')),
                             'value':html.ObjectDefinition(appmodel,djp,\
                                          self.pannel_data(monitor))})
            if workers:
                for worker in workers:
                    monitors.append(
                            {'name':worker.pop('aid','worker'),
                            'value':html.ObjectDefinition(appmodel,djp,\
                                            self.pannel_data(worker))})
        servers = [{'name':'Server',
                    'value':html.ObjectDefinition(appmodel,djp,\
                                   self.pannel_data(info['server']))}]
        return {'left_panels':servers,
                'right_panels':monitors}

    
class PulsarServerApplication(AdminApplication):
    inherit = True
    form = ServerForm
    list_per_page = 100
    converters = {'uptime': nicetimedelta}
    list_display = ('code','path','machine','this','notes')
    object_widgets = views.extend_widgets({'home':ServerView()})
     

################################    TASKQUEUE DJPCMS APPLICATION

task_display = (
    html.table_header('name','name',function='nice_name'),
    'status','timeout','time_executed','time_start','time_end',
    html.table_header('task_duration','duration',function='duration'),
    'expiry',
    'api',
    'user')


class JobsView(views.SearchView):
    astable = True
     
    def linkname(self, djp):
        return 'Job list'
    
    def title(self, djp):
        try:
            p = self.appmodel.proxy(djp.request)
            return 'Job list on {0}'.format(p.domain)
        except:
            return 'No Jobs'


class JobApplication(views.ModelApplication):
    proxy = None
    list_display = ('name','type','next_run','run_every','runs_count','doc')
    table_actions = [views.application_action('bulk_run','run', djpcms.ADD)]
    search = JobsView()
    task_header = ('name','status','user','time_executed','id')
    
    def basequery(self, djp):
        p = self.proxy(djp.request)
        try:
            jobs = p.job_list()
        except:
            return 'No connection'
        return sorted((JobModel(p,name,self.list_display,data) for\
                       name,data in jobs),key = lambda x : x.name)        
    
    def ajax__bulk_run(self, djp):
        request = djp.request
        data = request.REQUEST
        if 'ids[]' in data:
            taskapp = djp.site.for_model(Task)
            p = self.proxy(djp.request)
            body = []
            for job in data.getlist('ids[]'):
                try:
                    res = p.run_new_task(job)
                except:
                    continue
                if 'id' in res:
                    id = res['id']
                    task = Task.objects.get(id = id)
                    url = taskapp.viewurl(request,task)
                    res['time_executed'] = smart_time(res['time_executed'])
                    id = id[:8]
                    if url:
                        id = html.Widget('a',href=url).render(inner=id)
                    res['id'] = id
                body.append([res.get(head,None) for head in self.task_header])
            inner = html.Table(self.task_header,
                               body = body,
                               footer = False,
                               data = {'options':{'sDom':'t'}}).render(djp)
            return ajax.dialog(hd = 'Executed Tasks', bd = inner,
                               modal = True,
                               width = 700)
    
    
class TasksAdmin(AdminApplicationSimple):
    list_display = ('short_id',) + task_display
    object_display = ('id',) + task_display +\
                     ('string_result','stack_trace') 
    has_plugins = False
    inherit = True
    proxy = None
    
    view = views.ViewView(regex = views.UUID_REGEX)
    
    #redisdb = JobApplication('/jobs/', JobModel, parent = 'search')
                

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
        
        
