import logging
import os
import sys
import traceback

import pulsar
from pulsar.utils.py2py3 import execfile
from pulsar.utils.importer import import_module
#from pulsar.utils import debug

__all__ = ['Application',
           'require']


def require(appname):
    apps = appname.split('.')
    if len(apps) == 1:
        module = 'pulsar.apps.{0}'.format(appname)
    else:
        module = appname
    mod = import_module(module)
    return mod


class Application(pulsar.PickableMixin):
    """\
    An application interface for configuring and loading
    the various necessities for any given server application
    
:parameter callable: A callable which return the application server.
                     The callable must be pickable, therefore it is either a function
                     or a pickable object.
    """
    default_logging_level = logging.INFO
    REMOVABLE_ATTRIBUTES = ('_pulsar_arbiter',)
    
    def __init__(self, callable = None, usage=None, cfg = None, **params):
        self.usage = usage
        self.cfg = cfg
        self.callable = callable
        self.load_config(**params)
        arbiter = pulsar.arbiter()
        arbiter.add_monitor(pulsar.WorkerMonitor,self)
    
    def add_timeout(self, deadline, callback):
        self.arbiter.ioloop.add_timeout(deadline, callback)
        
    def load_config(self, parse_console = True, **params):
        '''Load the application configuration'''
        self.cfg = pulsar.Config(self.usage)
        
        # add params
        for k, v in params.items():
            k = k.lower()
            try:
                self.cfg.set(k, v)
            except AttributeError:
                setattr(self,k,v)
                
        # parse console args
        if parse_console:
            parser = self.cfg.parser()
            opts, args = parser.parse_args()
        else:
            parser, opts, args = None,None,None
        
        # optional settings from apps
        cfg = self.init(parser, opts, args)
        
        # Load up the any app specific configuration
        if cfg:
            for k, v in list(cfg.items()):
                self.cfg.set(k.lower(), v)
                
        # Load up the config file if its found.
        if parser:
            if opts.config and os.path.exists(opts.config):
                cfg = {
                    "__builtins__": __builtins__,
                    "__name__": "__config__",
                    "__file__": opts.config,
                    "__doc__": None,
                    "__package__": None
                }
                try:
                    execfile(opts.config, cfg, cfg)
                except Exception:
                    print("Failed to read config file: %s" % opts.config)
                    traceback.print_exc()
                    sys.exit(1)
            
                for k, v in cfg.items():
                    # Ignore unknown names
                    if k not in self.cfg.settings:
                        continue
                    try:
                        self.cfg.set(k.lower(), v)
                    except:
                        sys.stderr.write("Invalid value for %s: %s\n\n" % (k, v))
                        raise
                
            # Lastly, update the configuration with any command line
            # settings.
            for k, v in opts.__dict__.items():
                if v is None:
                    continue
                self.cfg.set(k.lower(), v)
               
    def init(self, parser = None, opts = None, args = None):
        pass
    
    def load(self):
        raise NotImplementedError

    def reload(self):
        self.load_config()
        loglevel = self.LOG_LEVELS.get(self.cfg.loglevel.lower(), logging.INFO)
        self.log.setLevel(loglevel)
        
    def handler(self):
        '''Returns a callable application handler,
used by a :class:`pulsar.Worker` to carry out its task.'''
        if self.callable is None:
            self.callable = self.load()
        return self.callable
    
    def worker_task(self, worker):
        '''Callback by worker class when the worker when it
receives the arbiter proxy.

:parameter worker: the :class:`pulsar.Worker` which received the callback
                   and where ``self`` is served from. '''
        pass
    
    def start(self):
        '''Start the application'''
        pulsar.arbiter().start()
        return self
            
    def stop(self):
        arbiter = getattr(self,'_pulsar_arbiter',None)
        if arbiter:
            arbiter.stop()
    
    def configure_logging(self):
        """\
        Set the log level and choose the destination for log output.
        """
        if self.cfg.debug:
            self.loglevel = logging.DEBUG
        else:
            self.loglevel = self.cfg.loglevel
        handlers = []
        if self.cfg.logfile and self.cfg.logfile != "-":
            handlers.append(logging.FileHandler(self.cfg.logfile))
        super(Application,self).configure_logging(handlers = handlers)

