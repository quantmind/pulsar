from djpcms import forms
from djpcms.template import loader
from djpcms.contrib.monitor import views
from djpcms.apps.included.admin import AdminApplication
from djpcms.html import LazyRender, Table, ObjectDefinition
from djpcms.utils import mark_safe

from stdnet.orm import model_iterator

import pulsar
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
     
    def get_client(self, instance):
        return rpc.JsonProxy('http://{0}:{1}'.\
                             format(instance.host,instance.port))
        
    def render_object(self, djp):
        instance = djp.instance
        change = self.getview('change')(djp.request, **djp.kwargs)
        r = self.get_client(instance)
        try:
            info = r.server_info()
        except pulsar.ConnectionError:
            panels = [{'name':'Server','value':'No Connection'}]
            databases = ''
        else:
            databases = Table(djp, **info.pop('keys')).render()
            panels = ({'name':k,'value':ObjectDefinition(self,djp,v)} for k,v in info.items())
        view = loader.render(self.template_view,
                            {'panels':panels,
                             'databases':databases})
        ctx = {'view':view,
               'change':change.render()}
        return loader.render(self.view_template,ctx)
    