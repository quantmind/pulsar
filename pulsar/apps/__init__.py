import logging
import os
import sys
from inspect import isfunction

import pulsar
from pulsar import Actor, Monitor, get_actor, maybe_async_deco, EventHandler,\
                    QueueServer, QueueTransport
from pulsar.utils.importer import module_attribute
from pulsar.utils.pep import pickle

__all__ = ['Application',
           'CPUboundApplication',
           'Worker',
           'ApplicationMonitor',
           'get_application']


class TaskQueueFactory(pulsar.Setting):
    app = 'cpubound'
    name = "task_queue_factory"
    section = "Task Consumer"
    flags = ["-q", "--task-queue"]
    default = "multiprocessing.queues.Queue"
    desc = """The task queue factory to use."""

    def get(self):
        return module_attribute(self.value)
    
        
def get_application(name):
    '''Invoked in the arbiter domain, this function will return
the :class:`Application` associated with *name* if available. If not in the
:class:`Arbiter` domain it returns nothing.'''
    actor = get_actor()
    if actor and actor.is_arbiter():
        monitor = actor.monitors.get(name)
        if monitor:
            return getattr(monitor, 'app', None)
        
def monitor_start(self):
    self.app.monitor_start(self)
    if not self.cfg.workers:
        self.app.worker_start(self)
    self.app.fire_event('start')

def monitor_info(self, data):
    if not self.cfg.workers:
        self.app.worker_info((self, data))
    else:
        self.app.monitor_info((self, data))
        
def monitor_stop(self):
    if not self.cfg.workers:
        self.app.worker_stop(self)
    self.app.monitor_stop(self)


class Worker(Actor):
    """\
An :class:`Actor` for serving a pulsar :class:`Application`.

.. attribute:: app

    Instance of the :class:`Application` served by the worker
"""
    def __init__(self, *args, **kwargs):
        super(Worker, self).__init__(*args, **kwargs)
        self.bind_event('start', self.app.worker_start)
        self.bind_event('stop', self.app.worker_stop)
        self.bind_event('info', self.app.worker_info)
        
    @property
    def app(self):
        return self.params.app
    
    def io_poller(self):
        return self.app.io_poller(self)
    

class ApplicationMonitor(Monitor):
    '''A :class:`Monitor` for managing a pulsar :class:`Application`.
    
.. attribute:: app_handler

    The monitor application handler obtained from
    :meth:`Application.monitor_handler`.
'''
    actor_class = Worker
    
    def __init__(self, *args, **kwargs):
        super(ApplicationMonitor, self).__init__(*args, **kwargs)
        self.bind_event('start', monitor_start)
        self.bind_event('stop', monitor_stop)
        self.bind_event('info', monitor_info)
        
    @property
    def app(self):
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


class AppEvents(EventHandler):
    ONE_TIME_EVENTS = ('ready', 'start', 'stop')
    MANY_TIMES_EVENTS = ('info',)
    
    
class Application(pulsar.Pulsar):
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
:parameter name: Application name. If not provided the class name in lower
    case is used
:parameter params: a dictionary of configuration parameters which overrides
    the defaults and the `cfg` attribute. They will be overritten by
    a config file or command line arguments.

.. attribute:: app

    A string indicating the application namespace for configuration parameters.

    Default: ``None``.

.. attribute:: callable

    A callable serving your application. The callable must be pickable,
    therefore it is either a function
    or a pickable object. If not provided, the application must
    implement the :meth:`handler` method.

    Default ``None``

.. attribute:: cfg

    dictionary of default configuration parameters.

    Default: ``{}``.

.. attribute:: cfg_apps

    Optional tuple containing names of configuration namespaces to
    be included in the application config dictionary.

    Default: ``None``
    
.. attribute:: script

    full path of the script which starts the application or ``None``.
    If supplied it is used to setup the python path
"""
    cfg = {}
    _app_name = None
    description = None
    epilog = None
    cfg_apps = None
    config_options_include = None
    config_options_exclude = None

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
        self._app_name = self._app_name or self.__class__.__name__.lower()
        self._name = name or self._app_name
        self.script = script
        self.python_path()
        params = cfg or {}
        if self.cfg:
            params.update(self.cfg)
        params.update(kwargs)
        self.callable = callable
        self.load_config(argv, version, parse_console, params)
        self()

    def __call__(self, actor=None):
        if actor is None:
            actor = get_actor()
        monitor = None
        if actor and actor.is_arbiter():
            monitor = actor.monitors.get(self.name)
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
        return self
    
    @property
    def app_name(self):
        return self._app_name

    @property
    def name(self):
        '''Application name, It is unique and defines the application.'''
        return self._name
    
    def __repr__(self):
        return self.name

    def __str__(self):
        return self.__repr__()

    @property
    def monitor(self):
        actor = get_actor()
        if actor:
            return actor.monitors.get(self.name)

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
        '''called by :meth:`Actor.io_pooler`.'''
        return None
    
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

    def python_path(self):
        #Insert the application directory at the top of the python path.
        fname = self.script or os.getcwd()
        path = os.path.split(fname)[0]
        if path not in sys.path:
            sys.path.insert(0, path)

    def add_timeout(self, deadline, callback):
        self.arbiter.ioloop.add_timeout(deadline, callback)
    
    def load_config(self, argv, version, parse_console, params):
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
        cfg_apps = set(self.cfg_apps or ())
        cfg_apps.add(self.app_name)
        self.cfg_apps = cfg_apps
        cfg = pulsar.Config(self.description,
                            self.epilog,
                            version,
                            self.cfg_apps,
                            self.config_options_include,
                            self.config_options_exclude)
        self.cfg = self.on_config_init(cfg, params)
        if not isinstance(self.cfg, pulsar.Config):
            self.cfg = cfg
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
                    self.cfg.set(k, v)
                    self.cfg.settings[k].default = v
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

    def monitor_handler(self):
        '''Returns an application handler for the :class:`ApplicationMonitor`.
By default it returns ``None``.'''
        return None
    
    # WORKERS CALLBACKS
    def worker_start(self, worker):
        '''Called by the :class:`Worker` :meth:`pulsar.Actor.on_start`
:ref:`callback <actor-callbacks>` method.'''
        pass

    def worker_info(self, arg):
        pass
    
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

    def monitor_info(self, worker, data):
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


class CPUboundApplication(Application):
    '''A CPU-bound :class:`Application` is an application which
handles events with a task to complete and the time complete the task is
determined principally by the speed of the CPU.
This type of application is served by :ref:`CPU bound workers <cpubound>`.'''
    _app_name = 'cpubound'
    
    def __init__(self, *args, **kwargs):
        self.received = 0
        self.concurrent_requests = set()
        super(CPUboundApplication, self).__init__(*args, **kwargs)
        
    def io_poller(self, worker):
        server = QueueServer(consumer_factory=self.request_instance,
                             backlog=self.cfg.backlog)
        worker.servers[self.name] = server
        return server.poller(worker.params.queue)
    
    @property
    def transport(self):
        if self.local.transport is None:
            self.local.transport = QueueTransport(self.local.queue)
        return self.local.transport
    
    def put(self, request):
        '''Put a *request* into the :attr:`transport` if available.'''
        self.transport.write(request)

    def request_instance(self, request):
        '''Build a request class from a *request*. By default it returns the
request. This method is called by the :meth:`on_request` once a new
request has been obtained from the :attr:`ioqueue`.'''
        return request

    def worker_start(self, worker):
        self.transport.event_loop = worker.requestloop
        worker.servers[self.name].connection_made(self.transport)
    
    def monitor_info(self, worker, data):
        tq = self.ioqueue
        if tq is not None:
            if isinstance(tq, Queue):
                tqs = 'multiprocessing.Queue'
            else:
                tqs = str(tq)
            try:
                size = tq.qsize()
            except NotImplementedError: #pragma    nocover
                size = 0
            data['queue'] = {'ioqueue': tqs, 'ioqueue_size': size}
        return data
    
    def actorparams(self, monitor, params):
        if 'queue' not in self.local:
            self.local.queue = self.cfg.task_queue_factory()
        params['queue'] = self.local.queue
        return params
