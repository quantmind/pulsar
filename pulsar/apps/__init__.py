'''
This module implements the main classes for pulsar application framework. The
framework is built on top of pulsar asynchronous engine and allows to
implement servers with very little effort. The main classes here are:
:class:`Application` and :class:`MultiApp` which, has the name suggests, is
a factory of several :class:`Application` running on a single server.
The :class:`Configurator` is a mixin used as base class for both 
:class:`Application` and :class:`MultiApp`.

An instance of an :class:`Application` is pickable and therefore can be sent
from actor to actor using the
:ref:`actor message passing api <tutorials-messages>`.

Configurator
===============================
   
.. autoclass:: Configurator
   :members:
   :member-order: bysource
   
   
Application
===============================
   
.. autoclass:: Application
   :members:
   :member-order: bysource
   

Multi App
===============================
      
.. autoclass:: MultiApp
   :members:
   :member-order: bysource
   
   
Get application
=========================

.. autofunction:: get_application

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
'''
import os
import sys
import logging
from inspect import getfile

import pulsar
from pulsar import Actor, Monitor, get_actor, EventHandler, QueueServer
from pulsar.utils.importer import module_attribute
from pulsar.utils.structures import OrderedDict
from pulsar.utils.config import section_docs
from pulsar.utils.pep import pickle

__all__ = ['Application',
           'CPUboundApplication',
           'MultiApp',
           'Worker',
           'ApplicationMonitor',
           'get_application']


section_docs['Task Consumer'] = '''
This section covers configuration parameters used by CPU bound type applications
such as the :ref:`distributed task queue <apps-taskqueue>` and the
:ref:`test suite <apps-test>`, or in general, application which derive from
:class:`pulsar.apps.CPUboundApplication`.
'''

class TaskQueueFactory(pulsar.Setting):
    app = 'cpubound'
    name = "task_queue_factory"
    section = "Task Consumer"
    flags = ["-q", "--task-queue-factory"]
    default = "pulsar.PythonMessageQueue"
    desc = '''\
        Dotted path to the task queue factory to use.

        A task queue factory is callable (either a function or a class)
        which accepts one parameter only and returns an instance of a
        distributed queue which has the same API as
        :class:`pulsar.MessageQueue`. The only parameter passed to the
        task queue factory is a :class:`pulsar.utils.config.Config` instance.
        This parameters is used by :class:`pulsar.apps.CPUboundApplication`
        such as the :ref:`distributed task queue <apps-taskqueue>` and the
        :ref:`test suite <apps-test>`.
        The default value is the :class:`pulsar.PythonMessageQueue`.'''

    def get(self):
        return module_attribute(self.value)
    
    
def get_application(name):
    '''Fetch the :class:`Application` associated with *name* if available. This
function return a generator and therefore it must be handled by the
coroutine::

    app = yield = pulsar.get_application('taskqueue')
    
'''
    actor = get_actor()
    if actor:
        if actor.is_arbiter():
            yield _get_app(actor, name)
        else:
            yield actor.send('arbiter', 'run', _get_app, name)
        
def _get_app(arbiter, name):
    monitor = arbiter.get_actor(name)
    if monitor:
        return monitor.params.app
        
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
    '''A :class:`pulsar.Monitor` for managing a pulsar :class:`Application`.'''
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
    '''A mixin for configuring and loading the various necessities for any
given server running on :mod:`pulsar` concurrent framework.

:parameter name: to override the class :attr:`name` attribute.
:parameter description: to override the class :attr:`cfg.description` attribute.
:parameter epilog: to override the class :attr:`cfg.epilog` attribute.
:parameter version: Optional version of this application, it overrides the
    class :attr:`cfg.version` attribute.
:parameter argv: Optional list of command line parameters to parse, if
    not supplied the :attr:`sys.argv` list will be used. The parameter is
    only relevant if **parse_console** is ``True``.
:parameter parse_console: ``True`` (default) if the console parameters needs
    parsing.
:parameter script: Optional string which set the :attr:`script` attribute.
:parameter params: a dictionary of configuration parameters which overrides
    the defaults and the :attr:`cfg` class attribute. They will be overritten
    by a :ref:`config file <setting-config>` or command line
    arguments.   

.. attribute:: name
    
    The name is unique if this is an :class:`Application`. In this
    case it defines the application monitor name as well and can be access in
    the arbiter domain via the :func:`get_application` function.
    
.. attribute:: argv

    Optional list of command line parameters. If not available the
    :attr:`sys.argv` list will be used when parsing the console.
    
.. attribute:: cfg

    The :class:`pulsar.utils.config.Config` for this :class:`Configurator`.
    If set as class attribute it will be replaced during initialisation.

    Default: ``None``.

.. attribute:: parsed_console

    ``True`` if this application parsed the console before starting.
    
.. attribute:: script

    Full path of the script which starts the application or ``None``.
    Evaluated during initialization via the :meth:`python_path` method.
'''
    name = None
    cfg = None
    
    def __init__(self,
                 name=None,
                 description=None,
                 epilog=None,
                 version=None,
                 argv=None,
                 parse_console=True,
                 script=None,
                 cfg=None,
                 load_config=True,
                 **kwargs):
        cls = self.__class__
        self.name = name or cls.name or cls.__name__.lower()
        if load_config or not isinstance(cfg, pulsar.Config):
            cfg = cfg or {}
            cfg.update(kwargs)
            cfg = cls.create_config(cfg)
        self.cfg = cfg
        self.cfg.description = description or self.cfg.description
        self.cfg.epilog = epilog or self.cfg.epilog
        self.cfg.version = version or self.cfg.version
        self.argv = argv
        self.parsed_console = parse_console
        self.script = self.python_path(script)
    
    @property
    def version(self):
        '''Version of this :class:`Application`'''
        return self.cfg.version
    
    @property
    def root_dir(self):
        '''Root directory of this :class:`Configurator`. Evaluated from the
:attr:`script` attribute.'''
        if self.script:
            return os.path.dirname(self.script)
    
    def __repr__(self):
        return self.name

    def __str__(self):
        return self.__repr__()
    
    def python_path(self, script):
        '''Called during initialization to obtain the ``script`` name and
to add the :attr:`script` directory to the python path if not in the
path already.
If ``script`` does not evalueate to ``True`` it is evaluated from
the ``__main__`` import. Returns the real path of the python
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
    
    def on_config(self):
        '''Callback when configuration is loaded. This is a chance to do
 an application specific check before the concurrent machinery is put into
 place. If it returns ``False`` the application will abort.'''
        pass
    
    def load_config(self):
        '''Load the application configuration from a file and/or
from the command line. Called during application initialization.
The parameters overriding order is the following:

 * default parameters.
 * the *params* passed in the initialization.
 * the parameters in the optional configuration file
 * the parameters passed in the command line.
'''
        # get the actor if available and override default cfg values with those
        # from the actor
        actor = get_actor()
        if actor and actor.running:
            # actor available and running. unless argv is set, skip parsing
            if self.argv is None:
                self.parsed_console = False
            for k, v in actor.cfg.items():
                if v is not None:
                    k = k.lower()
                    try:
                        self.cfg.set(k, v, default=True)
                    except AttributeError:
                        pass
        # parse console args 
        if self.parsed_console:
            parser = self.cfg.parser()
            opts = parser.parse_args(self.argv)
            config = getattr(opts, 'config', None)
            # set the config only if config is part of the settings
            if config is not None and self.cfg.config:
                self.cfg.config = config
        else:
            parser, opts = None, None
        #
        # Load up the config file if its found.
        self.cfg.params.update(self.cfg.import_from_module())
        #
        # Update the configuration with any command line settings.
        if opts:
            for k, v in opts.__dict__.items():
                if v is None:
                    continue
                self.cfg.set(k.lower(), v)
    
    @classmethod
    def create_config(cls, params, prefix=None, dont_prefix=None, name=None):
        '''Create a new :class:`pulsar.utils.config.Config` container by
overriding default values with *params*.'''
        if cls.cfg:
            cfg = cls.cfg.copy(name=name, prefix=prefix,
                               dont_prefix=dont_prefix)
        else:
            cfg = pulsar.Config(name=name, prefix=prefix,
                                dont_prefix=dont_prefix)
        for name, value in params.items():
            if name in cfg:
                cfg.set(name, value, default=True)
            else:
                cfg.params[name] = value
        return cfg
    
    
class Application(Configurator, pulsar.Pulsar):
    """An application interface for configuring and loading
the various necessities for any given server or distributed application running
on :mod:`pulsar` concurrent framework.
Applications can be of any sorts or forms and the library is shipped with
several battery included examples in the :mod:`pulsar.apps` framework module.

These are the most important facts about a pulsar :class:`Application`

* It derives from :class:`Configurator` so that it has all the functionalities
  to parse command line arguments and setup the :attr:`Configurator.cfg`.
* Instances must be pickable. If non-pickable data needs to be add on an
  :class:`Application` instance, it must be stored on the
  :attr:`Application.local` dictionary.
* When a new :class:`Application` is initialized,
  a new :class:`ApplicationMonitor` instance is added to the
  :class:`Arbiter`, ready to perform its duties.

:parameter callable: Initialise the :attr:`Application.callable` attribute.
:parameter load_config: If ``False`` the :meth:`Configurator.load_config`
    is not invoked. Default ``True``.
:parameter params: Passed to the :class:`Configurator` initialiser.

.. attribute:: callable

    Optional callable serving or configuring your application.
    If provided, the callable must be pickable, therefore it is either
    a function or a pickable object.

    Default ``None``
"""
    def __init__(self, callable=None, load_config=True, **params):
        '''Initialize a new :class:`Application` and add its
:class:`ApplicationMonitor` to the class:`pulsar.Arbiter`.

:parameter version: Optional version number of the application.

    Default: ``pulsar.__version__``

:parameter parse_console: flag for parsing console inputs. By default it parse
    only if the arbiter has not yet started.
'''
        super(Application, self).__init__(load_config=load_config, **params)
        self.local.events = AppEvents()
        self.callable = callable
        if load_config:
            self.load_config()
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
before a new :class:`Worker` is spawned. This method is invoked by
the :class:`ApplicationMonitor`. By default it does nothing.'''
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


class CPUboundApplication(Application):
    '''A CPU-bound :class:`Application` is an application which
handles events with a task to complete and the time complete the task is
determined principally by the speed of the CPU.
This type of application is served by :ref:`CPU bound workers <cpubound>`.'''
    def __init__(self, *args, **kwargs):
        self.received = 0
        self.concurrent_requests = set()
        super(CPUboundApplication, self).__init__(*args, **kwargs)
        
    def io_poller(self, worker):
        '''Create the queue server and the IO poller for the *worker*
:class:`pulsar.EventLoop`.'''
        # Get the task queue from the params container
        self.local.queue = worker.params.queue
        # Build the Queue server
        server = QueueServer(consumer_factory=self.request_instance,
                             backlog=self.cfg.backlog)
        worker.servers[self.name] = server
        return self.queue.poller(server)
    
    @property
    def queue(self):
        '''Distributed :class:`pulsar.MessageQueue`.'''
        return self.local.queue
    
    def put(self, request):
        '''Put a *request* into the :attr:`transport` if available.'''
        self.queue.put(request)

    def request_instance(self, request):
        '''Build a request instance from a *request*. By default it returns the
*request*. This method is called by the :class:`pulsar.QueueServer`
once a new request has been obtained from the distributed :attr:`queue`.'''
        return request

    def monitor_start(self, monitor):
        '''Create the :attr:`queue` from the
:ref:`task-queue-factory setting <setting-task_queue_factory>`.'''
        self.local.queue = self.cfg.task_queue_factory(self.cfg)
        
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
The :class:`MultiApp` derives from :class:`Configurator` and therefore
supports all its configuration utilities.

A minimal example usage::

    import pulsar
    
    class Server(pulsar.MultiApp):
        def build(self):
            yield self.new_app(TaskQueue)
            yield self.new_app(WSGIserver, prefix=rpc, callable=...)
            yield self.new_app(WSGIserver, prefix=web, callable=...)
'''
    _apps = None
    
    def build(self):
        '''Virtual method, must be implemented by subclasses and return an
iterable over results obtained from calls to the :meth:`new_app` method.'''
        raise NotImplementedError
    
    def apps(self):
        '''List of :class:`Application` for this :class:`MultiApp`.
The list is lazily loaded from the :meth:`build` method.'''
        if self._apps is None:
            # Add non-default settings values to the list of cfg params
            self.cfg.params.update(((s.name, s.value) for s in\
                       self.cfg.settings.values() if s.value != s.default))
            self.cfg.settings = {}
            self._apps =  []
            apps = OrderedDict(self.build())
            if not apps:
                return self._apps
            # Load the configuration (command line and config file)
            self.load_config()
            kwargs = self._get_app_params()
            for App, name, callable, cfg in self._iter_app(apps):
                settings = self.cfg.settings
                new_settings = {}
                for key in cfg:
                    setting = settings[key].copy()
                    if setting.orig_name and setting.orig_name != setting.name:
                        setting.name = setting.orig_name
                    new_settings[setting.name] = setting
                cfg.settings = new_settings
                kwargs.update({'name': name, 'cfg': cfg, 'callable': callable})
                self._apps.append(App(**kwargs))
        return self._apps
    
    def new_app(self, App, prefix=None, dont_prefix=None,
                callable=None, **kwargs):
        '''Invoke this method in the :meth:`build` method as many times
as the number of :class:`Application` required by this :class:`MultiApp`.'''
        params = self.cfg.params.copy()
        params.update(kwargs)
        name = params.pop('name', None)
        prefix = prefix or ''
        if not prefix:
            name = self.name
            cfg = App.create_config(params, name=name)
        else:
            name = '%s_%s' % (self.name, prefix)
            cfg = App.create_config(params, prefix=prefix,
                                    dont_prefix=dont_prefix, name=name)
        self.cfg.settings.update(cfg.settings)
        return prefix, (App, name, callable, cfg)
            
    def __call__(self, actor=None):
        return pulsar.multi_async((app(actor) for app in self.apps()))
        
    def start(self):
        '''Use this method to start all applications at once.'''
        apps = self.apps()
        arbiter = pulsar.arbiter()
        if arbiter:
            arbiter.start()
    
    ##    INTERNALS
    def _iter_app(self, app_name_callables):
        main = app_name_callables.pop('', None)
        if not main:
            raise pulsar.ImproperlyConfigured('No main application in MultiApp')
        yield main
        for app in app_name_callables.values():
            yield app
    
    def _get_app_params(self):
        params = self.cfg.params.copy()
        for key, value in self.__dict__.items():
            if key.startswith('_'):
                continue
            elif key == 'parsed_console':
                key = 'parse_console'
            params[key] = value
        params['load_config'] = False
        return params