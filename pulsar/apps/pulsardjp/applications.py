from djpcms import forms
from djpcms.template import loader
from djpcms.contrib.monitor import views
from djpcms.apps.included.admin import AdminApplication
from djpcms.html import LazyRender, Table, ObjectDefinition
from djpcms.utils.dates import nicetimedelta
from djpcms.utils.text import nicename
from djpcms.utils import mark_safe

import pulsar
from pulsar.utils.py2py3 import iteritems
from pulsar.http import rpc


class ServerForm(forms.Form):
    host = forms.CharField(initial = 'localhost')
    port = forms.IntegerField(initial = 8060)
    notes = forms.CharField(widget = forms.TextArea,
                            required = False)
    

class PulsarServerApplication(AdminApplication):
    inherit = True
    form = ServerForm
    list_per_page = 100
    template_view = ('pulsardjp/monitor.html',)
    converters = {'uptime': nicetimedelta}
     
    def get_client(self, instance):
        return rpc.JsonProxy('http://{0}:{1}'.\
                             format(instance.host,instance.port))
        
    def render_object(self, djp):
        instance = djp.instance
        change = self.getview('change')(djp.request, **djp.kwargs)
        r = self.get_client(instance)
        try:
            panels = self.get_panels(djp,r.server_info())
        except pulsar.ConnectionError:
            panels = [{'name':'Server','value':'No Connection'}]
        view = loader.render(self.template_view,panels)
        ctx = {'view':view,
               'change':change.render()}
        return loader.render(self.view_template,ctx)
    
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
