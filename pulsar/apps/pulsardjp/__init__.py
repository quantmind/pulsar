'''\
A pulsar application for serving djpcms_ powered web sites.
It includes an :class:`pulsar.apps.tasks.Task` implementation
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


def set_proxy_function(sites, proxy):
    for site in sites:
        for app in site.applications:
            if hasattr(app,'proxy') and app.proxy == None:
                app.proxy = proxy
                

class SiteLoader(object):
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
        pass
    
    def get_settings(self):
        import djpcms
        if self.loaded:
            return djpcms.sites.settings


class DjpCmsApplicationCommand(wsgi.WSGIApplication):
    _name = 'djpcms'
    
    def handler(self):
        '''Returns a callable application handler,
used by a :class:`pulsar.Worker` to carry out its task.'''
        callable = self.callable
        if callable:
            self.sites = callable()
        return self.load()
    
    def load(self):
        from djpcms import http
        self.sites.load()
        return http.DjpCmsHandler(self.sites)

    def configure_logging(self):
        pass
