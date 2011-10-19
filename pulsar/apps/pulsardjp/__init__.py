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
import sys

from pulsar.apps import wsgi

from .models import *
from .forms import *


def set_proxy_function(sites, proxy):
    for site in sites:
        for app in site.applications:
            if hasattr(app,'proxy') and app.proxy == None:
                app.proxy = proxy
                

class SiteLoader(object):
    '''An pickable utility for loading and configuring djpcms_ sites before 
 pulsar server starts. It can be used as the callable to be be passed to
 the ``run_pulsar`` command.
 
 .. attribute:: name
 
     The configuration name, useful when different types of configuration are
     needed (WEB, RPC, ...)
     

A tipical usage along these lines::

    import djpcms
    from pulsar.apps import pulsardjp
    
    pt.add(module='siropy', down = ('clients',))
    
    class SiteLoader(pulsardjp.SiteLoader):
        ...
    
    if __name__ == '__main__':
        return djpcms.execute(SiteLoader('WEB'))
 '''
    settings = None
    ENVIRON_NAME = 'PULSAR_SERVER_TYPE'
    
    def __init__(self, name):
        self.loaded = None
        self.name = name
        
    def __getstate__(self):
        d = self.__dict__.copy()
        d['loaded'] = None
        return d
        
    def __call__(self):
        import djpcms
        if not self.loaded:
            os.environ[self.ENVIRON_NAME] = self.name
            self.loaded = True
            name = '_load_{0}'.format(self.name.lower())
            func = getattr(self,name,self._load)
            func()
            sites = djpcms.sites
            djpcms.init_logging(sites.settings)
            sites.load()
            self.finish(djpcms.sites)
        return djpcms.sites
            
    def _load(self):
        import djpcms
        djpcms.MakeSite(os.getcwd(),
                        settings = self.settings)

    def finish(self, sites):
        '''Callback once the sites are loaded.'''
        pass
    
    def wsgi(self):
        '''This function is invoked by self at the end of its callable
function to create the WSGI handler.'''
        sites = self.__call__()
        from djpcms import http
        return http.WSGI(sites)
        
    def get_settings(self):
        import djpcms
        if self.loaded:
            return djpcms.sites.settings


class DjpCmsWSGIApplication(wsgi.WSGIApplication):
    _name = 'djpcms'
    
    def handler(self):
        '''Returns a callable application handler,
used by a :class:`pulsar.Worker` to carry out its task.'''
        return self.callable.wsgi()
