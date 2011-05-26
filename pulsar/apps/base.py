import logging
import os
import sys
import traceback

import pulsar
from pulsar import Empty
from pulsar.utils.py2py3 import execfile
from pulsar.utils.importer import import_module
#from pulsar.utils import debug

__all__ = ['Application',
           'require']


def require(appname):
    '''Shortcut function to load an application'''
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
the various necessities for any given server application.
    
:parameter callable: A callable which return the application server.
                     The callable must be pickable, therefore it is either a function
                     or a pickable object.
:parameter actor_links: A dictionary actor proxies.
:parameter params: a dictionary of configuration parameters which overrides the defaults.
"""
    cfg = {}
    _name = None
    monitor_class = pulsar.WorkerMonitor
    default_logging_level = logging.INFO
    
    def __init__(self,
                 callable = None,
                 usage=None,
                 name = None,
                 **params):
        self.python_path()
        self._name = name or self._name
        self.usage = usage
        nparams = self.cfg.copy()
        nparams.update(params)
        self.callable = callable
        self.load_config(**nparams)
        arbiter = pulsar.arbiter(self.cfg.daemon)
        self.mid = arbiter.add_monitor(self.monitor_class,
                                       self,
                                       self.name).aid
    
    @property
    def name(self):
        return self._name or self.__class__.__name__.lower()
    
    def python_path(self):
        path = os.path.split(os.getcwd())[0]
        if path not in sys.path:
            sys.path.insert(0, path)
            
    def add_timeout(self, deadline, callback):
        self.arbiter.ioloop.add_timeout(deadline, callback)
        
    def load_config(self, parse_console = True, **params):
        '''Load the application configuration'''
        self.cfg = pulsar.Config(self.usage)
        
        overrides = {}
        
        # add params
        for k, v in params.items():
            if v is not None:
                k = k.lower()
                try:
                    self.cfg.set(k, v)
                except AttributeError:
                    if not self.add_to_overrides(k,v,overrides):
                        setattr(self,k,v)
        
        config = self.cfg.config
        
        # parse console args
        if parse_console:
            parser = self.cfg.parser()
            opts, args = parser.parse_args()
            config = opts.config or config
        else:
            parser, opts, args = None,None,None
        
        # optional settings from apps
        cfg = self.init(parser, opts, args)
        
        # Load up the any app specific configuration
        if cfg:
            for k, v in list(cfg.items()):
                self.cfg.set(k.lower(), v)
        
        # Load up the config file if its found.
        if config and os.path.exists(config):
            cfg = {
                "__builtins__": __builtins__,
                "__name__": "__config__",
                "__file__": config,
                "__doc__": None,
                "__package__": None
            }
            try:
                execfile(config, cfg, cfg)
            except Exception:
                print("Failed to read config file: %s" % config)
                traceback.print_exc()
                sys.exit(1)
        
            for k, v in cfg.items():
                # Ignore unknown names
                if k not in self.cfg.settings:
                    self.add_to_overrides(k,v,overrides)
                else:
                    try:
                        self.cfg.set(k.lower(), v)
                    except:
                        sys.stderr.write("Invalid value for %s: %s\n\n" % (k, v))
                        raise
            
        # Update the configuration with any command line settings.
        if opts:
            for k, v in opts.__dict__.items():
                if v is None:
                    continue
                self.cfg.set(k.lower(), v)
                
        # Lastly, update the configuration with overrides
        for k,v in overrides.items():
            self.cfg.set(k, v)
            
    
    def add_to_overrides(self, name, value, overrides):
        names = name.lower().split('_')
        if len(names) > 1 and names[0] == self.name:
            name = '_'.join(names[1:])
            if name in self.cfg.settings:
                overrides[name] = value
                return True
            
    def init(self, parser = None, opts = None, args = None):
        pass
    
    def load(self):
        pass
        
    def handler(self):
        '''Returns a callable application handler,
used by a :class:`pulsar.Worker` to carry out its task.'''
        return self.load() or self.callable
    
    def monitor_task(self, monitor):
        '''Callback by :class:`pulsar.WorkerMonitor`` at each event loop'''
        pass
    
    def worker_task(self, worker):
        '''Callback by and instance of :class:`pulsar.Worker`` class
at each ``worker`` event loop.'''
        if worker.task_queue:
            try:
                args = worker.task_queue.get(timeout = 0.1)
            except Empty:
                return
            except IOError:
                return
            worker.handle_task(*args)
            
    def handle_event_task(self, worker, request):
        '''And a task event. Called by the worker to perform the application task.'''
        raise NotImplementedError
    
    def end_event_task(self, worker, response, result):
        ''''''
        pass
    
    def get_task_queue(self):
        return None
    
    def start(self):
        '''Start the application'''
        pulsar.arbiter().start()
        return self
            
    def stop(self):
        '''Stop the application.'''
        arbiter = pulsar.arbiter()
        monitor = arbiter.get_monitor(self.mid)
        if monitor:
            monitor.stop()
            
    def on_exit(self, worker):
        pass
    
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

    def actor_links(self, links):
        if not links:
            raise StopIteration
        else:
            arbiter = pulsar.arbiter()
            for name,app in links.items():
                if app.mid in arbiter.monitors:
                    monitor = arbiter.monitors[app.mid]
                    monitor.actor_links[self.name] = self
                    yield name, app
            