'''\
A pulsar application for serving ``djpcms`` powered web sites.

To use it:

* Add ``pulsar.apps.pulsardjp`` to the list of ``INSTALLED_APPS``.
* type::

    python manage.py run_pulsar
        
'''
import os
import sys

import pulsar


class DjpCmsApplicationCommand(pulsar.Application):
    
    def __init__(self, sites, **kwargs):
        self.sites = sites
        super(DjpCmsApplicationCommand,self).__init__(**kwargs)
        
    def load(self):
        from djpcms.apps.handlers import DjpCmsHandler
        self.sites.load()
        return DjpCmsHandler(self.sites)

    def configure_logging(self):
        pass
        