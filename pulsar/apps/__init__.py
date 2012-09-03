import logging
import os
import sys
from inspect import isfunction

import pulsar
from pulsar import Actor, make_async, safe_async, is_failure, HaltServer,\
                     Monitor, loop_timeout, Deferred, get_actor
from pulsar.async.defer import pickle
from pulsar.utils.log import LogInformation

__all__ = ['Application',
           'MultiApp',
           'ApplicationHandlerMixin',
           'Worker',
           'ApplicationMonitor',
           'get_application']


class safe_monitor:
    '''Decorator for monitor and application functions'''    
    def __init__(self, event=None):
        self.event = event
    
    def _halt(self, failure, app):
        failure.log()
        if app.can_kill_arbiter:
            raise HaltServer('Unhandled exception application.')
        else:
            return failure

    def signal_event(self, result, app):
        if self.event:
            app.events[self.event].callback(result)
        return result
    
    def __call__(self, f):
        def _(*args, **kwargs):
            app = args[0].app
            result = safe_async(f, args, kwargs)
            res = result.add_callback(lambda r: app,
                                      lambda f: self._halt(f, app))
            return res.addBoth(lambda r: self.signal_event(r, app))
        _.__name__ = f.__name__
        _.__doc__ = f.__doc__
        return _


def get_application(name):
    '''Invoked in the arbiter domain, this function will return
the :class:`Application` associated with *name* if available. If not in the
:class:`Arbiter` domain it returns nothing.'''
    actor = get_actor()
    if actor and actor.is_arbiter():
        monitor = actor._monitors.get(name)
        if monitor:
            return getattr(monitor, 'app', None) 
    
    
class ApplicationHandlerMixin(object):
    '''A mixin for both :class:`Worker` and :class:`ApplicationMonitor`.
It implements the :meth:`handle_request` actor method
used for by the :class:`Application` for handling requests and
sending back responses.
'''
    def on_event(self, fd, event):
        '''Override :meth:`pulsar.Actor.on_event` to delegate handling
to the underlying :class:`Application`.'''
        return self.app.on_event(self, fd, event)

    def handle_task(self):
        if self.information.log():
            self.log.info('Processed {0} requests'.format(self.nr))
        try:
            self.cfg.worker_task(self)
        except:
            pass
        self.app.worker_task(self)

    def configure_logging(self, config=None):
        # Delegate to application
        self.app.configure_logging(config=config)
        self.loglevel = self.app.loglevel
        self.setlog()


class Worker(ApplicationHandlerMixin, Actor):
    """\
An :class:`Actor` class for serving an :class:`Application`.
It provides two new methods inherited from :class:`ApplicationHandlerMixin`.

.. attribute:: app

    Instance of the :class:`Application` to be performed by the worker

.. attribute:: cfg

    Configuration dictionary

.. attribute:: app_handler

    The application handler obtained from :meth:`Application.handler`.

"""
    @property
    def class_code(self):
        return 'worker %s' % self.app.name
    
    def on_init(self, app=None, **kwargs):
        self.app = app
        self.cfg = app.cfg
        self.max_requests = self.cfg.max_requests or sys.maxsize
        self.information = LogInformation(self.cfg.logevery)
        self.app_handler = app.handler()
        return kwargs

    # Delegates Callbacks to the application

    def on_start(self):
        self.app.worker_start(self)
        try:
            self.cfg.worker_start(self)
        except:
            pass

    def on_task(self):
        self.handle_task()

    def on_stop(self):
        return self.app.worker_stop(self)

    def on_exit(self):
        self.app.worker_exit(self)
        try:
            self.cfg.worker_exit(self)
        except:
            pass

    def on_info(self, info):
        return self.app.on_info(self,info)


class ApplicationMonitor(ApplicationHandlerMixin, Monitor):
    '''A specialized :class:`Monitor` implementation for managing
pulsar subclasses of :class:`Application`.
'''
    # For logging name
    @property
    def class_code(self):
        return 'monitor %s' % self.app.name
    
    def on_init(self, app=None, **kwargs):
        self.app = app
        self.cfg = app.cfg
        self.max_requests = 0
        arbiter = pulsar.arbiter()
        # Set the arbiter config object if not available
        if not arbiter.cfg:
            arbiter.cfg = app.cfg
        self.max_requests = None
        self.information = LogInformation(self.cfg.logevery)
        app.monitor_init(self)
        if not self.cfg.workers:
            self.app_handler = app.handler()
        else:
            self.app_handler = app.monitor_handler()
        kwargs['actor_class'] = Worker
        kwargs['num_actors'] = app.cfg.workers
        return super(ApplicationMonitor, self).on_init(**kwargs)

    ############################################################################
    # Delegates Callbacks to the application
    
    @safe_monitor('on_start')
    def on_start(self):
        self.app.monitor_start(self)
        # If no workears are available invoke the worker start method too
        if not self.cfg.workers:
            self.app.worker_start(self)

    @safe_monitor()
    def monitor_task(self):
        yield self.app.monitor_task(self)
        # There are no workers, the monitor do their job
        if not self.cfg.workers:
            yield self.handle_task()

    def on_stop(self):
        if not self.cfg.workers:
            yield self.app.worker_stop(self)
        yield self.app.monitor_stop(self)
        yield super(ApplicationMonitor, self).on_stop()
        self.app.events['on_stop'].callback(self.app)

    def on_exit(self):
        self.app.monitor_exit(self)
        try:
            self.cfg.worker_exit(self)
        except:
            pass

    def clean_up(self):
        self.worker_class.clean_arbiter_loop(self,self.ioloop)

    def actorparams(self):
        '''Override the :meth:`Monitor.actorparams` method to
updated actor parameters with information about the application.

:rtype: a dictionary of parameters to be passed to the
    spawn method when creating new actors.'''
        p = Monitor.actorparams(self)
        app = self.app
        impl = app.cfg.concurrency
        if impl == 'thread':
            app = pickle.loads(pickle.dumps(app))
        p.update({'app': app,
                  'timeout': app.cfg.timeout,
                  'loglevel': app.loglevel,
                  'max_concurrent_requests': app.cfg.backlog,
                  'concurrency': impl,
                  'name':'{0}-worker'.format(app.name)})
        return app.actorparams(self, p)

    def on_info(self, info):
        info.update({'default_timeout': self.cfg.timeout,
                     'max_requests': self.cfg.max_requests})
        return self.app.on_info(self,info)


class Application(pulsar.LogginMixin):
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
    
.. attribute:: mid

    The unique id of the :class:`ApplicationMonitor` managing the
    application. Defined at runtime.

.. attribute:: script

    full path of the script which starts the application or ``None``.
    If supplied it is used to setup the python path

.. attribute:: can_kill_arbiter

    If ``True``, an unhandled error in the application will shut down the
    :class:`pulsar.Arbiter`. Check the :meth:`ApplicationMonitor.monitor_task`
    method for implementation.

    Default: ``False``.


.. attribute:: remotes

    Optiona :class:`pulsar.RemoteMethods` class to provide additional
    remote functions to be added to the monitor dictionary of remote functions.

    Default: ``None``.
"""
    cfg = {}
    _app_name = None
    description = None
    mid = None
    epilog = None
    cfg_apps = None
    config_options_include = None
    config_options_exclude = None
    can_kill_arbiter = False
    commands_set = None
    monitor_class = ApplicationMonitor

    def __init__(self,
                 callable=None,
                 description=None,
                 name=None,
                 epilog=None,
                 argv=None,
                 script=None,
                 version=None,
                 can_kill_arbiter=None,
                 parse_console=True,
                 **params):
        '''Initialize a new :class:`Application` and add its
:class:`ApplicationMonitor` to the class:`pulsar.Arbiter`.

:parameter version: Optional version number of the application.

    Default: ``pulsar.__version__``

:parameter parse_console: flag for parsing console inputs. By default it parse
    only if the arbiter has not yet started.
'''
        self.description = description or self.description
        if can_kill_arbiter is not None:
            self.can_kill_arbiter = bool(can_kill_arbiter)
        self.epilog = epilog or self.epilog
        self._app_name = self._app_name or self.__class__.__name__.lower()
        self._name = name or self._app_name
        self.script = script
        self.python_path()
        nparams = self.cfg.copy()
        nparams.update(params)
        self.callable = callable
        self.load_config(argv, version, parse_console, nparams)
        self()

    def __call__(self, actor=None):
        if actor is None:
            actor = get_actor()
        if not self.mid and (not actor or actor.is_arbiter()):
            # Add events
            self.local.events = {'on_start': Deferred(), 'on_stop': Deferred()}
            self.configure_logging()
            if self.on_config() is not False:
                arbiter = pulsar.arbiter(self.cfg.daemon)
                monitor = arbiter.add_monitor(self.monitor_class,
                                              self.name,
                                              app=self,
                                              ioqueue=self.ioqueue)
                self.mid = monitor.aid
                if self.commands_set:
                    monitor.commands_set.update(self.commands_set)
        events = self.events
        if events:
            return events['on_start']

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
    def monitor(self):
        if self.mid:
            return pulsar.arbiter()._monitors.get(self.mid)

    @property
    def events(self):
        return self.local.events
    
    def __repr__(self):
        return self.name

    def __str__(self):
        return self.name

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

    def put(self, request):
        queue = self.ioqueue
        if queue:
            self.log.debug('Put {0} on IO queue'.format(request))
            queue.put(('request',request))
        else:
            self.log.error("Trying to put a request on task queue,\
 but there isn't one!")

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

:parameter parse_console: if ``False`` the console won't be parsed.
:parameter params: parameters which override the defaults.

The parameters overrriding order is the following:

 * default parameters.
 * the *params* passed in the initialization.
 * the parameters in the optional configuration file
 * the parameters passed in the command line.
'''
        cfg_apps = set(self.cfg_apps or ())
        cfg_apps.add(self.app_name)
        self.cfg_apps = cfg_apps
        self.cfg = pulsar.Config(self.description,
                                 self.epilog,
                                 version,
                                 self.cfg_apps,
                                 self.config_options_include,
                                 self.config_options_exclude)
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
                        setattr(self,k,v)
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
        '''Returns a application handler for the monitor.
By default it returns ``None``.'''
        return None

    # MONITOR AND WORKER CALLBACKS
    def on_info(self, worker, data):
        return data

    def on_event(self, worker, fd, events):
        pass

    def worker_start(self, worker):
        '''Called by the :class:`Worker` :meth:`pulsar.Actor.on_start`
:ref:`callback <actor-callbacks>` method.'''
        pass

    def worker_task(self, worker):
        '''Callback by the *worker* :meth:`Actor.on_task` callback.'''
        return

    def worker_stop(self, worker):
        '''Called by the :class:`Worker` just after stopping.'''
        pass

    def worker_exit(self, worker):
        '''Called by the :class:`Worker` just when exited.'''
        pass

    # MONITOR CALLBAKS
    def actorparams(self, monitor, params):
        '''A chance to override the actor parameters before a new
:class:`Worker` is spawned'''
        return params

    def monitor_init(self, monitor):
        '''Callback by :class:`ApplicationMonitor` when initializing.
This is a chance to setup your application before the application
monitor is added to the arbiter.'''
        pass

    def monitor_start(self, monitor):
        '''Callback by :class:`ApplicationMonitor` when starting.
The application is now in the arbiter but has not yet started.'''
        pass

    def monitor_task(self, monitor):
        '''Callback by :class:`ApplicationMonitor` at each event loop'''
        pass

    def monitor_stop(self, monitor):
        '''Callback by :class:`ApplicationMonitor` at each event loop'''
        pass

    def monitor_exit(self, monitor):
        '''Callback by :class:`ApplicationMonitor` at each event loop'''
        pass

    def start(self):
        '''Start the application if it wasn't already started.'''
        arbiter = pulsar.arbiter()
        if self.name in arbiter.monitors:
            arbiter.start()
        return self

    def stop(self):
        '''Stop the application.'''
        arbiter = pulsar.arbiter()
        monitor = arbiter.get_monitor(self.mid)
        if monitor:
            monitor.stop()

    def configure_logging(self, config = None):
        """Set the logging configuration as specified by the
 :ref:`logconfig <setting-logconfig>` setting."""
        self.loglevel = self.cfg.loglevel
        config = config or self.cfg.logconfig
        super(Application,self).configure_logging(config=config)

    def actorlinks(self, links):
        if not links:
            raise StopIteration
        else:
            arbiter = pulsar.arbiter()
            for name,app in links.items():
                if app.mid in arbiter.monitors:
                    monitor = arbiter.monitors[app.mid]
                    monitor.actor_links[self.name] = self
                    yield name, app


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