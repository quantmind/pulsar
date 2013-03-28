'''
Application
===============================
   
.. autoclass:: Application
   :members:
   :member-order: bysource
   

CPU bound Application
===============================
      
.. autoclass:: CPUboundApplication
   :members:
   :member-order: bysource


Application Worker
===============================
   
.. autoclass:: Worker
   :members:
   :member-order: bysource


Application Monitor
===============================
   
.. autoclass:: ApplicationMonitor
   :members:
   :member-order: bysource

Multi App
===============================
      
.. autoclass:: MultiApp
   :members:
   :member-order: bysource
   
'''
import os
import sys
import logging
from inspect import getfile

import pulsar
from pulsar import Actor, Monitor, get_actor, EventHandler, QueueServer
from pulsar.utils.importer import module_attribute
from pulsar.utils.pep import pickle

__all__ = ['Application',
           'CPUboundApplication',
           'MultiApp',
           'Worker',
           'ApplicationMonitor',
           'get_application']


class TaskQueueFactory(pulsar.Setting):
    app = 'cpubound'
    name = "task_queue_factory"
    section = "Task Consumer"
    flags = ["-q", "--task-queue"]
    default = "pulsar.PythonMessageQueue"
    desc = """The task queue factory to use."""

    def get(self):
        return module_attribute(self.value)
    
        
def get_application(name):
    '''Invoked in the arbiter domain, this function will return
the :class:`Application` associated with *name* if available. If not in the
:class:`Arbiter` domain it returns nothing.'''
    actor = get_actor()
    if actor and actor.is_arbiter():
        monitor = actor.registered.get(name)
        if monitor:
            return getattr(monitor, 'app', None)
        
def monitor_start(self):
    self.app.monitor_start(self)
    if not self.cfg.workers:
        self.app.worker_start(self)
    self.app.fire_event('start')
        
def monitor_stop(self):
    if not self.cfg.workers:
        self.app.worker_stop(self)
    self.app.monitor_stop(self)


class Worker(Actor):
    '''An :class:`pulsar.Actor` for serving a pulsar :class:`Application`.'''
    def __init__(self, *args, **kwargs):
        super(Worker, self).__init__(*args, **kwargs)
        self.bind_event('start', self.app.worker_start)
        self.bind_event('stop', self.app.worker_stop)
        
    @property
    def app(self):
        '''The :class:`Application` served by this :class:`Worker`.'''
        return self.params.app
    
    def io_poller(self):
        '''Delegates the :meth:`pulsar.Actor.io_poller` method to the
:meth:`Application.io_poller` method of the :attr:`app` attribute.'''
        return self.app.io_poller(self)
    
    def info(self):
        data = super(Worker, self).info()
        return self.app.worker_info(self, data)
    

class ApplicationMonitor(Monitor):
    '''A :class:`Monitor` for managing a pulsar :class:`Application`.'''
    actor_class = Worker
    
    def __init__(self, *args, **kwargs):
        super(ApplicationMonitor, self).__init__(*args, **kwargs)
        self.bind_event('start', monitor_start)
        self.bind_event('stop', monitor_stop)
        
    @property
    def app(self):
        '''The :class:`Application` served by this
:class:`ApplicationMonitor`.'''
        return self.params.app
        
    ############################################################################
    # Delegates Callbacks to the application
    def monitor_task(self):
        self.app.monitor_task(self)
        
    def actorparams(self):
        p = Monitor.actorparams(self)
        app = self.app
        if self.cfg.concurrency == 'thread':
            app = pickle.loads(pickle.dumps(app))
        p.update({'app': app,
                  'name': '{0}-worker'.format(app.name)})
        return self.app.actorparams(self, p)
    
    def info(self):
        data = super(ApplicationMonitor, self).info()
        if not self.cfg.workers:
            return self.app.worker_info(self, data)
        else:
            return self.app.monitor_info(self, data)
        


class AppEvents(EventHandler):
    ONE_TIME_EVENTS = ('ready', 'start', 'stop')
    

class Configurator(object):
    cfg = {}
    description = None
    epilog = None
    version = None
    cfg_apps = frozenset()
    config_options_include = None
    config_options_exclude = None
    
    def on_config_init(self, cfg, params):
        '''Callback when configuration is initialised but not yet loaded.
This is a chance to add extra :ref:`config parameters <settings>` or remove
unwanted ones. It returns a new :class:`Config` instance or ``None``.'''
        pass
    
    def on_config(self):
        '''Callback when configuration is loaded. This is a chance to do
 an application specific check before the concurrent machinery is put into
 place. If it returns ``False`` the application will abort.'''
        pass
    
    def load_config(self, argv, version, parse_console,
                    cfg_apps=None, settings=None, **params):
        '''Load the application configuration from a file and/or
from the command line. Called during application initialization.

:parameter argv: list of command line parameters to parse.
:parameter version: The version of this application.
:parameter parse_console: True if the console parameters need parsing.
:parameter params: dictionary of parameters passed during construction.

The parameters overriding order is the following:

 * default parameters.
 * the *params* passed in the initialization.
 * the parameters in the optional configuration file
 * the parameters passed in the command line.
'''
        cfg_apps = set(cfg_apps or ())
        cfg_apps.update(self.cfg_apps)
        self.cfg_apps = frozenset(cfg_apps)
        cfg = pulsar.Config(self.description,
                            self.epilog,
                            version or self.version,
                            self.cfg_apps,
                            self.config_options_include,
                            self.config_options_exclude,
                            settings=settings)
        self.cfg = self.on_config_init(cfg, params)
        if not isinstance(self.cfg, pulsar.Config):
            self.cfg = cfg
        self.version = self.cfg.version
        overrides = {}
        specials = set()
        # get the actor if available and override default cfg values with those
        # from the actor
        actor = get_actor()
        if actor and actor.running:
            # actor available and running. unless argv is set, skip parsing
            if argv is None:
                parse_console = False
            for k, v in actor.cfg.items():
                if v is not None:
                    k = k.lower()
                    try:
                        self.cfg.set(k, v)
                        self.cfg.settings[k].default = v
                    except AttributeError:
                        pass
        # modify defaults and values of cfg with params
        for k, v in params.items():
            if v is not None:
                k = k.lower()
                try:
                    self.cfg.set(k, v, default=True)
                except AttributeError:
                    if not self.add_to_overrides(k, v, overrides):
                        setattr(self, k, v)
        # parse console args
        if parse_console:
            parser = self.cfg.parser()
            opts = parser.parse_args(argv)
            config = getattr(opts, 'config', None)
            # set the config only if config is part of the settings
            if config is not None and self.cfg.config:
                self.cfg.config = config
        else:
            parser, opts = None, None
        #
        # Load up the config file if its found.
        for k, v in self.cfg.import_from_module():
            self.add_to_overrides(k, v, overrides)
        #
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
        names = name.split('__')
        if len(names) == 2 and names[0] == self.name:
            name = names[1].lower()
            if name in self.cfg.settings:
                overrides[name] = value
                return True
    
    
    
class Application(pulsar.Pulsar, Configurator):
    """An application interface for configuring and loading
the various necessities for any given server or distributed application running
on :mod:`pulsar` concurrent framework.
Applications can be of any sorts or forms and the library is shipped with
several battery included examples in the :mod:`pulsar.apps` framework module.

These are the most important facts about a pulsar :class:`Application`

* Instances must be pickable. If non-pickable data needs to be add on an
  :class:`Application` instance, it must be stored on the
  :attr:`Application.local` dictionary.
* When a new :class:`Application` is initialized,
  a new :class:`ApplicationMonitor` instance is added to the
  :class:`Arbiter`, ready to perform its duties.

:parameter callable: Initialise the :attr:`Application.callable` attribute.
:parameter description: A string describing the application.
    It will be displayed on the command line.
:parameter epilog: Epilog string you will see when interacting with the command
    line.
:parameter name: Application name. Override the :attr:`name` class attribute.
:parameter params: a dictionary of configuration parameters which overrides
    the defaults and the :attr:`cfg` class attribute. They will be overritten
    by a :ref:`config file <setting-config>` or command line
    arguments.

.. attribute:: callable

    Optional callable serving or configuring your application.
    If provided, the callable must be pickable, therefore it is either
    a function or a pickable object.

    Default ``None``

.. attribute:: cfg

    dictionary of default configuration parameters. It will be replaced by
    a :class:`pulsar.utils.config.Config` container during initialization.

    Default: ``{}``.

.. attribute:: cfg_apps

    Optional tuple\set containing names of
    :ref:`configuration namespaces <settings>` to be included in the
    application config dictionary.

    Default: Empty ``frozenset``.
    
.. attribute:: script

    full path of the script which starts the application or ``None``.
    If supplied it is used to setup the python path
"""
    def __init__(self,
                 callable=None,
                 description=None,
                 name=None,
                 epilog=None,
                 argv=None,
                 script=None,
                 version=None,
                 parse_console=True,
                 cfg=None,
                 **kwargs):
        '''Initialize a new :class:`Application` and add its
:class:`ApplicationMonitor` to the class:`pulsar.Arbiter`.

:parameter version: Optional version number of the application.

    Default: ``pulsar.__version__``

:parameter parse_console: flag for parsing console inputs. By default it parse
    only if the arbiter has not yet started.
'''
        self.local.events = AppEvents()
        self.description = description or self.description
        self.epilog = epilog or self.epilog
        self._name = self.__class__.name or self.__class__.__name__.lower()
        self.script = self.python_path(script)
        params = cfg or {}
        if self.cfg:
            params.update(self.cfg)
        params.update(kwargs)
        self.callable = callable
        self.load_config(argv, version, parse_console, **params)
        self()

    def __call__(self, actor=None):
        if actor is None:
            actor = get_actor()
        monitor = None
        if actor and actor.is_arbiter():
            monitor = actor.get_actor(self.name)
        if monitor is None and (not actor or actor.is_arbiter()):
            self.cfg.on_start()
            self.configure_logging()
            self.fire_event('ready')
            arbiter = pulsar.arbiter(cfg=self.cfg.new_config())
            if self.on_config() is not False:
                monitor = arbiter.add_monitor(ApplicationMonitor,
                                              self.name,
                                              app=self,
                                              cfg=self.cfg)
                self.cfg = monitor.cfg
        return self.event('start')
        
    @property
    def app(self):
        '''Returns ``self``. Implemented so that the :class:`ApplicationMonitor`
and the :class:`Application` have the same interface.'''
        return self
    
    @property
    def name(self):
        '''Application name, It is unique and defines the application.'''
        return self._name
    
    @property
    def root_dir(self):
        if self.script:
            return os.path.dirname(self.script)
    
    def __repr__(self):
        return self.name

    def __str__(self):
        return self.__repr__()

    @property
    def monitor(self):
        actor = get_actor()
        if actor:
            return actor.registered.get(self.name)

    def __setstate__(self, state):
        super(Application, self).__setstate__(state)
        self.local.events = AppEvents()
        
    def fire_event(self, name):
        return self.local.events.fire_event(name, self)
        
    def bind_event(self, name, callback):
        return self.local.events.bind_event(name, callback)
        
    def event(self, name):
        return self.local.events.event(name)

    def io_poller(self, worker):
        '''Called by :meth:`Worker.io_poller` method during the initialization
of the :class:`Worker` event loop. By default it does nothing so that
the event loop chooses the most suitable IO poller.'''
        return None

    def python_path(self, script):
        '''Get the script name if not available and the script directory to the
python path if not already there. Returns thereal path of the python
script which runs the application.'''
        if not script:
            try:
                import __main__
            except ImportError:
                return
            script = getfile(__main__)
        script = os.path.realpath(script)
        path = os.path.dirname(script)
        if path not in sys.path:
            sys.path.insert(0, path)
        return script

    def add_timeout(self, deadline, callback):
        self.arbiter.ioloop.add_timeout(deadline, callback)
    
    # WORKERS CALLBACKS
    def worker_start(self, worker):
        '''Called by the :class:`Worker` :meth:`pulsar.Actor.on_start`
:ref:`callback <actor-callbacks>` method.'''
        pass

    def worker_info(self, worker, data):
        return data
    
    def worker_stop(self, worker):
        '''Called by the :class:`Worker` :meth:`pulsar.Actor.on_stop`
:ref:`callback <actor-callbacks>` method.'''
        pass

    # MONITOR CALLBACKS
    def actorparams(self, monitor, params):
        '''A chance to override the dictionary of parameters *params*
before a new :class:`Worker` is spawned.'''
        return params

    def monitor_start(self, monitor):
        '''Callback by :class:`ApplicationMonitor` when starting.
The application is now in the arbiter but has not yet started.'''
        pass

    def monitor_info(self, monitor, data):
        return data
    
    def monitor_stop(self, monitor):
        '''Callback by :class:`ApplicationMonitor` when stopping'''
        pass

    def monitor_task(self, monitor):
        '''Callback by :class:`ApplicationMonitor` at each event loop'''
        pass
    
    def start(self):
        '''Start the application if it wasn't already started.'''
        arbiter = pulsar.arbiter()
        if arbiter and self.name in arbiter.registered:
            arbiter.start()
        return self
    
    @classmethod
    def create_config(cls, params, prefix=None, dont_prefix=None):
        kwargs = cls.cfg.copy()
        kwargs.update(params)
        cfg = pulsar.Config(app=cls.cfg_apps, prefix=prefix,
                            dont_prefix=dont_prefix)
        for name in params:
            if name in cfg:
                cfg.set(name, params[name], default=True)
        return cfg


class CPUboundApplication(Application):
    '''A CPU-bound :class:`Application` is an application which
handles events with a task to complete and the time complete the task is
determined principally by the speed of the CPU.
This type of application is served by :ref:`CPU bound workers <cpubound>`.'''
    cfg_apps = frozenset(('cpubound',))
    
    def __init__(self, *args, **kwargs):
        self.received = 0
        self.concurrent_requests = set()
        super(CPUboundApplication, self).__init__(*args, **kwargs)
        
    def io_poller(self, worker):
        '''Create the queue server and the IO poller for the *worker*
:class:`EventLoop`.'''
        self.local.queue = worker.params.queue
        server = QueueServer(consumer_factory=self.request_instance,
                             backlog=self.cfg.backlog)
        worker.servers[self.name] = server
        return self.queue.poller(server)
    
    @property
    def queue(self):
        return self.local.queue
    
    def put(self, request):
        '''Put a *request* into the :attr:`transport` if available.'''
        self.queue.put(request)

    def request_instance(self, request):
        '''Build a request class from a *request*. By default it returns the
request. This method is called by the :meth:`on_request` once a new
request has been obtained from the :attr:`ioqueue`.'''
        return request

    def monitor_start(self, monitor):
        '''Create the :attr:`queue` from the config dictionary.'''
        self.local.queue = self.cfg.task_queue_factory()
        
    def worker_start(self, worker):
        #once the worker starts we set the queue event loop
        self.queue.event_loop = worker.requestloop
        worker.servers[self.name].connection_made(self.queue)
    
    def monitor_info(self, worker, data):
        tq = self.queue
        if tq is not None:
            data['queue'] = {'ioqueue': str(tq), 'ioqueue_size': tq.size()}
        return data
    
    def actorparams(self, monitor, params):
        params['queue'] = self.local.queue
        return params


class MultiApp(Configurator):
    '''A :class:`MultiApp` is a tool for creating several :class:`Application`
and starting them at once. It makes sure all :ref:`settings` for the
applications created are available in the command line.
The :meth:`build` is the only method which must be implemented by subclasses.
Check the :class:`examples.taskqueue.manage.server` class in the
:ref:`taskqueue example <tutorials-taskqueue>` for an example.

.. attribute:: name

    Optional name which can be used in the :meth:`build` method.
'''    
    def __init__(self, name='multi', **params):
        self.name = name
        self.cfg = self.cfg.copy()
        self.cfg.update(params)
        self._apps = None
        self._settings = {}
    
    def apps(self):
        '''List of :class:`Application` for this :class:`MultiApp`.
The lists is lazily loaded from the :meth:`build` method.'''
        if self._apps is None:
            self._apps =  []
            app_name_callables = list(self.build())
            params = {'description': self.description,
                      'epilog': self.epilog,
                      'settings': self._settings,
                      'version': self.version}
            for App, name, callable in app_name_callables:
                app = App(callable, name=name, **params)
                self._apps.append(app)
        return self._apps
    
    def new_app(self, App, prefix=None, dont_prefix=None,
                callable=None, **kwargs):
        '''Create an instance of :class:`Application` *App*.'''
        params = self.cfg.copy()
        params.update(kwargs)
        name = params.pop('name', None)
        if prefix is None:
            name = self.name
            cfg = App.create_config(params)
        else:
            cfg = App.create_config(params, prefix=prefix,
                                    dont_prefix=dont_prefix)
            name = '%s_%s' % (self.name, prefix)
        self._settings.update(cfg.settings)
        return (App, name, callable)
            
    def __call__(self, actor=None):
        return pulsar.multi_async((app(actor) for app in self.apps()))
    
    def build(self):
        '''Virtual method, must be implemented by subclasses and return an
iterable of :class:`Application`.'''
        raise NotImplementedError
        
    def start(self):
        '''Use this method to start all applications at once.'''
        apps = self.apps()
        arbiter = pulsar.arbiter()
        if arbiter:
            arbiter.start()
        