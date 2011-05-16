'''\
A pulsar application for serving djpcms_ powered web sites.

To use it:

* Add ``pulsar.apps.pulsardjp`` to the list of ``INSTALLED_APPS``.
* type::

    python manage.py run_pulsar


.. _djpcms: http://djpcms.com/
'''
import os
import sys

from pulsar.apps import wsgi


class SiteLoader(object):
    
    def __init__(self, name):
        self.loaded = None
        self.name = name
        
    def __call__(self):
        import djpcms
        if not self.loaded:
            os.environ['MYWEB_SERVER_TYPE'] = self.name
            self.loaded = True
            name = '_load_{0}'.format(self.name.lower())
            func = getattr(self,name,self._load)
            func()
            sites = djpcms.sites
            djpcms.init_logging(sites.settings)
            sites.load()
        return djpcms.sites
            
    def _load(self):
        djpcms.MakeSite(__file__)



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
        from djpcms.apps.handlers import DjpCmsHandler
        self.sites.load()
        return DjpCmsHandler(self.sites)

    def configure_logging(self):
        pass
        