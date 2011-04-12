'''\
A pulsar application for serving ``djpcms`` powered web sites.

To use it:

* Add ``pulsar.apps.pulsardjp`` to the list of ``INSTALLED_APPS``.
* type::

    python manage.py run_pulsar
        
'''
import os
import sys
import logging

import pulsar


class DjpCmsApplicationCommand(pulsar.Application):
    
    def __init__(self, sites, options):
        self.sites = sites
        self.usage = None
        self.cfg = None
        self.config_file = options.get("config") or ""
        self.options = options
        self.callable = None
        self.load_config()
        
    def load_config(self):
        self.cfg = pulsar.Config()
        
        if self.config_file and os.path.exists(self.config_file):
            cfg = {
                "__builtins__": __builtins__,
                "__name__": "__config__",
                "__file__": self.config_file,
                "__doc__": None,
                "__package__": None
            }
            try:
                execfile(self.config_file, cfg, cfg)
            except Exception as e:
                print("Failed to read config file: %s" % self.config_file)
                traceback.print_exc()
                sys.exit(1)
        
            for k, v in list(cfg.items()):
                # Ignore unknown names
                if k not in self.cfg.settings:
                    continue
                try:
                    self.cfg.set(k.lower(), v)
                except:
                    sys.stderr.write("Invalid value for %s: %s\n\n" % (k, v))
                    raise
        
        for k, v in list(self.options.items()):
            if k.lower() in self.cfg.settings and v is not None:
                self.cfg.set(k.lower(), v)
        
    def load(self):
        from djpcms.apps.handlers import DjpCmsHandler
        self.sites.load()
        return DjpCmsHandler(self.sites)

    def configure_logging(self):
        """\
        Set the log level and choose the destination for log output.
        """
        self.log = logging.getLogger()
        