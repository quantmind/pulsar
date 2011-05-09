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


class DjpCmsApplicationCommand(wsgi.WSGIApplication):
    _name = 'djpcms'
    
    def load(self):
        from djpcms.apps.handlers import DjpCmsHandler
        self.sites.load()
        return DjpCmsHandler(self.sites)

    def configure_logging(self):
        pass
        