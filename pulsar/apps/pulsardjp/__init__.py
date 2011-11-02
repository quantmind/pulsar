'''\
A pulsar application for serving djpcms_ powered web sites.
It includes a :class:`pulsar.apps.tasks.Task` implementation
with Redis backend which uses stdnet_.

To use it:

* Add ``pulsar.apps.pulsardjp`` to the list of ``INSTALLED_APPS``.
* type::

    python manage.py run_pulsar


.. _djpcms: https://github.com/lsbardel/djpcms
.. _stdnet: http://lsbardel.github.com/python-stdnet/
'''
import os

from .models import *
from .forms import *


def set_proxy_function(sites, proxy):
    for site in sites:
        for app in site.applications:
            if hasattr(app,'proxy') and app.proxy == None:
                app.proxy = proxy
                

class SiteLoader(object):
    '''An utility calss for pickable callable instances for loading and
configuring djpcms_ sites. It can be used as the callable to be be passed to
:class:`pulsar.apps.wsgi.WsgiApplication` class.
 
 .. attribute:: name
 
     The configuration name, useful when different types of configuration are
     needed (WEB, RPC, ...)
     

A tipical usage along these lines::

    import djpcms
    from pulsar.apps import pulsardjp
    
    class SiteLoader(pulsardjp.SiteLoader):
        ...
    
    if __name__ == '__main__':
        return djpcms.execute(SiteLoader('WEB'))
 '''
    settings = None
    ENVIRON_NAME = 'PULSAR_SERVER_TYPE'
    wsgifactory = True
    
    def __init__(self, name):
        self.loaded = None
        self.name = name
        
    def __getstate__(self):
        d = self.__dict__.copy()
        d['loaded'] = None
        return d
        
    def __call__(self):
        return self.sites()
    
    def sites(self):
        import djpcms
        if not self.loaded:
            self.loaded = True
            os.environ[self.ENVIRON_NAME] = self.name
            name = '_load_{0}'.format(self.name.lower())
            func = getattr(self,name,self._load)
            func()
            sites = djpcms.sites
            sites.load()
            self.finish(djpcms.sites)
        return djpcms.sites
            
    def _load(self):
        # Standard loading method
        import djpcms
        djpcms.MakeSite(os.getcwd(), settings = self.settings)

    def finish(self, sites):
        '''Callback once the sites are loaded.'''
        pass
        
    def get_settings(self):
        import djpcms
        if self.loaded:
            return djpcms.sites.settings

