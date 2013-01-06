import logging
import os
import sys
from inspect import isfunction

import pulsar
from pulsar import Actor, make_async, safe_async, is_failure, HaltServer,\
                     Monitor, loop_timeout, Deferred, get_actor, async
from pulsar.async.defer import pickle
from pulsar.utils import events
from pulsar.utils.log import LogInformation

__all__ = ['Application',
           'MultiApp',
           'Worker',
           'ApplicationMonitor',
           'get_application']

APP_EVENTS = ('start', 'stop')

def app_events(app):
    for name in APP_EVENTS:
        yield name, Deferred(description='%s.%s' % (app.name, name))
        
def fire_event(f):
    name = f.__name__[3:]
    if name not in APP_EVENTS:
        raise ValueError()
    
    def fire(result, app):
        if not is_failure(result):
            result = app
        else:
            result.log()
        events.fire(name, app)
        app.events[name].callback(app)
        
    def _(*args, **kwargs):
        app = args[0].params.app
        safe_async(f, args, kwargs).addBoth(lambda r: fire(r, app))
    
    _.__name__ = f.__name__
    _.__doc__ = f.__doc__
    return _
            
def get_application(name):
    '''Invoked in the arbiter domain, this function will return
the :class:`Application` associated with *name* if available. If not in the
:class:`Arbiter` domain it returns nothing.'''
    actor = get_actor()
    if actor and actor.is_arbiter():
        monitor = actor.monitors.get(name)
        if monitor:
            return getattr(monitor, 'app', None) 


class Worker(Actor):
    """\
An :class:`Actor` for serving a pulsar :class:`Application`.

.. attribute:: app

    Instance of the :class:`Application` to be performed by the worker

.. attribute:: cfg

    Configuration dictionary

.. attribute:: app_handler

    The application handler obtained from :meth:`Application.handler`.

"""
    @property
    def app(self):
        return self.params.app

    # Delegates Callbacks to the application
    def on_start(self):
        app = self.app
        self.app_handler = app.handler()
        self.app.worker_start(self)
        try:
            self.cfg.worker_start(self)
        except:
            pass

    def on_stop(self):
        return self.app.worker_stop(self)

    def on_exit(self):
        self.app.worker_exit(self)
        try:
            self.cfg.worker_exit(self)
        except:
            pass

    def on_info(self, info):
        return self.app.on_info(self, info)
    
    def on_event(self, fd, event):
        return self.app.on_event(self, fd, event)


class ApplicationMonitor(Monitor):
    '''A :class:`Monitor` for managing a pulsar :class:`Application`.
    
.. attribute:: app_handler

    The monitor application handler obtained from
    :meth:`Application.monitor_handler`.
'''
    actor_class = Worker

    @property
    def app(self):
        return self.params.app
        
    ############################################################################
    # Delegates Callbacks to the application
    @fire_event
    def on_start(self):
        super(ApplicationMonitor, self).on_start()
        app = self.app
        app.monitor_start(self)
        # If no workers available invoke the worker start method too
        if not self.cfg.workers:
            self.app_handler = app.handler()
            app.worker_start(self)
        else:
            self.app_handler = app.monitor_handler()

    @fire_event
    def on_stop(self):
        app = self.params.app
        if not self.cfg.workers:
            yield app.worker_stop(self)
        yield app.monitor_stop(self)
        yield super(ApplicationMonitor, self).on_stop()

    def on_exit(self):
        app = self.params.app
        app.monitor_exit(self)
        try:
            self.cfg.worker_exit(self)
        except:
            pass
    
    def monitor_task(self):
        self.app.monitor_task(self)
        
    def on_event(self, fd, event):
        return self.app.on_event(self, fd, event)
    
    def actorparams(self):
        p = Monitor.actorparams(self)
        app = self.params.app
        if self.cfg.concurrency == 'thread':
            app = pickle.loads(pickle.dumps(app))
        p.update({'app': app,
                  'name': '{0}-worker'.format(app.name)})
        return app.actorparams(self, p)

    def on_info(self, info):
        if not self.cfg.workers:
            return self.params.app.on_info(self, info)
        else:
            return info


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
:parameter commands_set: Initialise the :attr:`commands_set` attribute.
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

.. attribute:: commands_set

    Optional set of :ref:`remote actions <api-remote_commands>` available
    on :class:`Actor` created by this :class:`Application`.

    Default: ``None``.
"""
    cfg = {}
    _app_name = None
    description = None
    epilog = None
    cfg_apps = None
    config_options_include = None
    config_options_exclude = None
    commands_set = None

    def __init__(self,
                 callable=None,
                 description=None,
                 name=None,
                 epilog=None,
                 argv=None,
                 script=None,
                 version=None,
                 parse_console=True,
                 commands_set=None,
                 cfg=None,
                 **kwargs):
        '''Initialize a new :class:`Application` and add its
:class:`ApplicationMonitor` to the class:`pulsar.Arbiter`.

:parameter version: Optional version number of the application.

    Default: ``pulsar.__version__``

:parameter parse_console: flag for parsing console inputs. By default it parse
    only if the arbiter has not yet started.
'''
        self.description = description or self.description
        self.epilog = epilog or self.epilog
        self._app_name = self._app_name or self.__class__.__name__.lower()
        self._name = name or self._app_name
        if commands_set is not None:
            self.commands_set = commands_set
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
            # Add events
            self.local.events = dict(app_events(self))
            self.cfg.on_start()
            self.configure_logging()
            events.fire('ready', self)
            arbiter = pulsar.arbiter(cfg=self.cfg.new_config())
            if self.on_config() is not False:
                monitor = arbiter.add_monitor(ApplicationMonitor,
                                              self.name,
                                              app=self,
                                              cfg=self.cfg,
                                              ioqueue=self.ioqueue)
                self.cfg = monitor.cfg
                if self.commands_set:
                    monitor.impl.commands_set.update(self.commands_set)
        if self.events:
            return self.events['start']

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

    @property
    def events(self):
        return self.local.events
    
    def __repr__(self):
        return self.name

    def __str__(self):
        return self.__repr__()

    @property
    def monitor(self):
        actor = get_actor()
        if actor:
            return actor.monitors.get(self.name)
         
    @property
    def ioqueue(self):
        if 'queue' not in self.local:
            self.local.queue = self.get_ioqueue()
        return self.local.queue

    def handler(self):
        '''Returns the callable application handler which is stored in
:attr:`Worker.app_handler`, used by :class:`Worker` to carry out its task.
By default it returns the :attr:`Application.callable`.'''
        return self.callable

    def request_instance(self, worker, fd, event):
        '''Build a request class from a file descriptor *fd* and an *event*.
The returned request instance is passed to the :meth:`handle_request`
method.'''
        return event

    def handle_request(self, worker, request):
        '''This is the main function which needs to be implemented
by actual applications. It is called by the *worker* to handle
a *request*.

:parameter worker: the :class:`Worker` handling the request.
:parameter request: an application specific request object.
:rtype: It can be a generator, a :class:`Deferred` instance
    or the actual response.'''
        raise NotImplementedError()

    def get_ioqueue(self):
        '''Returns an I/O distributed queue for the application if one
is needed. If a queue is returned, the application :class:`Worker`
will have a :class:`IOLoop` instance based on the queue (via :class:`IOQueue`).

By default it returns ``None``.'''
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

    # MONITOR AND WORKER CALLBACKS
    def on_info(self, worker, data):
        return data

    def on_event(self, worker, fd, events):
        pass
    
    # WORKERS CALLBACKS
    def worker_start(self, worker):
        '''Called by the :class:`Worker` :meth:`pulsar.Actor.on_start`
:ref:`callback <actor-callbacks>` method.'''
        pass

    def worker_stop(self, worker):
        '''Called by the :class:`Worker` :meth:`pulsar.Actor.on_stop`
:ref:`callback <actor-callbacks>` method.'''
        pass

    def worker_exit(self, worker):
        '''Called by the :class:`Worker` :meth:`pulsar.Actor.on_exit`
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

    def monitor_stop(self, monitor):
        '''Callback by :class:`ApplicationMonitor` when stopping'''
        pass

    def monitor_exit(self, monitor):
        '''Callback by :class:`ApplicationMonitor` at exit'''
        pass

    def monitor_task(self, monitor):
        '''Callback by :class:`ApplicationMonitor` at each event loop'''
        pass
    
    def start(self):
        '''Start the application if it wasn't already started.'''
        arbiter = pulsar.arbiter()
        if arbiter and self.name in arbiter.monitors:
            arbiter.start()
        return self


class MultiApp:
    
    def __init__(self, name='taskqueue', **params):
        self.name = name
        self.params = params
        self.apps = []
        
    def __call__(self, actor=None):
        raise NotImplementedError()
        
    def start(self):
        for app in self.apps:
            app.start()