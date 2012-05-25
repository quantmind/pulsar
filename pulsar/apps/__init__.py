import logging
import os
import sys
import traceback
import random
from inspect import isgenerator, isfunction

import pulsar
from pulsar import Empty, make_async, safe_async, is_failure, HaltServer,\
                     deferred_timeout, ispy3k
from pulsar.async.defer import pickle
from pulsar.utils.importer import import_module
from pulsar.utils.log import LogInformation
from pulsar.utils import system
from pulsar.utils.config import Setting
#from pulsar.utils import debug

__all__ = ['Application',
           'ApplicationHandlerMixin',
           'Worker',
           'ApplicationMonitor']


if ispy3k:
    def execfile(filename, globals=None, locals=None):
        if globals is None:
            globals = sys._getframe(1).f_globals
        if locals is None:
            locals = sys._getframe(1).f_locals
        with open(filename, "r") as fh:
            exec(fh.read()+"\n", globals, locals)

def halt_server(f):
    '''Halt server decorator'''
    def _halt(failure):
        failure.log()
        raise HaltServer('Unhandled exception application.')
    
    def _(self, *args, **kwargs):
        try:
            result = make_async(f(self, *args, **kwargs))
        except Exception as e:
            result = make_async(e)
        if self.app.can_kill_arbiter:
            result.add_errback(_halt)
        return result
    _.__name__ = f.__name__
    _.__doc__ = f.__doc__
    return _
            

class ApplicationHandlerMixin(object):
    '''A mixin for both :class:`Worker` and :class:`ApplicationMonitor`.
It implements the :meth:`handle_request` actor method
used for by the :class:`Application` for handling requests and
sending back responses.
'''
    def on_message(self, message):
        self.app.on_actor_message(message)
        
    def on_message_processed(self, message, result):
        self.app.on_actor_message_processed(message, result)
    
    def on_event(self, fd, event):
        '''Override :meth:`pulsar.Actor.on_event` to delegate handling
to the underlying :class:`Application`.'''
        # Build the request instance
        request = self.app.request_instance(self, fd, event)
        # the request class may contain a timeout
        timeout = getattr(request, 'timeout', None)
        should_stop = self.max_requests and self.nr >= self.max_requests
        d = safe_async(self._response_generator, args=(request, should_stop))
        # if a timeout is available, we add the deferred to the requestloop
        return deferred_timeout(d, self.requestloop, timeout)
        
    def handle_task(self):
        if self.information.log():
            self.log.info('Processed {0} requests'.format(self.nr))
        try:
            self.cfg.worker_task(self)
        except:
            pass
        self.app.worker_task(self)
        
    def handle_message(self, sender, message):
        '''Handle a *message* from a *sender*.'''
        return self.app.handle_message(sender, self, message)
    
    def configure_logging(self, config = None):
        # Delegate to application
        self.app.configure_logging(config = config)
        self.loglevel = self.app.loglevel
        self.setlog()
    
    ################################################################# Internals
    def _response_generator(self, request, should_stop):
        yield safe_async(self.cfg.pre_request, args=(self, request))
        # We don't care if it fails
        response = safe_async(self.app.handle_request, args=(self, request))
        yield response
        response = response.result
        if is_failure(response):
            response.log(self.log)
        else:
            close = getattr(response, 'close', None)
            if hasattr(close,'__call__'):
                response = safe_async(close)
                yield response
                if is_failure(response.result):
                    response.result.log(self.log)
        yield safe_async(self.cfg.post_request, args=(self, request))
        if should_stop:
            self.log.info("Auto-restarting worker.")
            self.stop()
        

class Worker(ApplicationHandlerMixin, pulsar.Actor):
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
    def on_init(self, app = None, **kwargs):
        self.app = app
        self.cfg = app.cfg
        self.max_requests = self.cfg.max_requests or sys.maxsize
        self.information = LogInformation(self.cfg.logevery)
        self.app_handler = app.handler()
    
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
        info.update({'request processed': self.nr})
        return self.app.on_info(self,info)


class ApplicationMonitor(ApplicationHandlerMixin, pulsar.Monitor):
    '''A specialized :class:`Monitor` implementation for managing
pulsar subclasses of :class:`Application`.
'''
    # For logging name
    _class_code = 'appmonitor'
    
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
        super(ApplicationMonitor,self).on_init(**kwargs)
    
    # Delegates Callbacks to the application
    @halt_server    
    def on_start(self):
        self.app.monitor_start(self)
        if not self.cfg.workers:
            self.app.worker_start(self)            
        
    @halt_server
    def monitor_task(self):
        yield self.app.monitor_task(self)
        # There are no workers, the monitor do their job
        if not self.cfg.workers:
            yield self.handle_task()
            
    def on_stop(self):
        # First we stop the application
        try:
            result = self.app.monitor_stop(self)
        except Exception as e:
            result = e
        stop = lambda r: super(ApplicationMonitor, self).on_stop()
        return make_async(result).addBoth(stop)
        
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
        app = self.app
        c = app.cfg.concurrency
        if c == 'thread':
            app = pickle.loads(pickle.dumps(app))
        return  {'app': app,
                 'timeout': app.cfg.timeout,
                 'loglevel': app.loglevel,
                 'max_concurrent_requests': app.cfg.backlog,
                 'impl': c,
                 'name':'{0}-worker'.format(app.name)}
        
    def on_info(self, info):
        info.update({'default_timeout': self.cfg.timeout,
                     'max_requests': self.cfg.max_requests})
        return self.app.on_info(self,info)
    

class Application(pulsar.LogginMixin):
    """\
An application interface for configuring and loading
the various necessities for any given server or distributed application running
on :mod:`pulsar` concurrent framework.
Applications can be of any sort or form and the library is shipped with several
battery included examples in the :mod:`pulsar.apps` framework module.

When creating a new application, a new :class:`ApplicationMonitor`
instance is added to the :class:`Arbiter`, ready to perform
its duties.
    
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
                 **params):
        '''Initialize a new :class:`Application` and add its
:class:`ApplicationMonitor` to the class:`pulsar.Arbiter`.

:parameter version: Optional version number of the application.

    Default: ``pulsar.__version__``
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
        self.load_config(argv, version=version, **nparams)
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
        return self.name 
    
    @property
    def ioqueue(self):
        if 'queue' not in self.local:
            self.local['queue'] = self.get_ioqueue()
        return self.local['queue']
    
    def handler(self):
        '''Returns the callable application handler which is stored in
:attr:`Worker.app_handler`, used by :class:`Worker` to carry out its task.
By default it returns the :attr:`Application.callable`.'''
        return self.callable
    
    def handle_message(self, sender, receiver, message):
        '''Handle messages for the *receiver*.'''
        handler = getattr(self, 'actor_' + message.action, None)
        if handler:
            return handler(sender, receiver, *message.args, **message.kwargs)
        else:
            receiver.log.error('Unknown action ' + message.action)
        
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
              
    def load_config(self, argv, version = None,
                    parse_console = True, **params):
        '''Load the application configuration from a file and/or
from the command line. Called during application initialization.

:parameter parse_console: if ``False`` the console won't be parsed.
:parameter params: parameters which override the defaults.

The parameters overrriding order is the following:

 * default parameters.
 * the :attr:`cfg` attribute.
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
        
        # modify defaults and values of cfg with params
        for k, v in params.items():
            if v is not None:
                k = k.lower()
                try:
                    self.cfg.set(k, v)
                    self.cfg.settings[k].default = v
                except AttributeError:
                    if not self.add_to_overrides(k,v,overrides):
                        setattr(self,k,v)
        
        try:
            config = self.cfg.config
        except AttributeError:
            config = None
        
        # parse console args
        if parse_console:
            parser = self.cfg.parser()
            opts = parser.parse_args(argv)
            try:
                config = opts.config or config
            except AttributeError:
                config = None
        else:
            parser, opts = None,None
        
        # optional settings from apps
        cfg = self.init(opts)
        
        # Load up the any app specific configuration
        if cfg:
            for k, v in list(cfg.items()):
                self.cfg.set(k.lower(), v)
        
        # Load up the config file if its found.
        if config and os.path.exists(config):
            #cfg = {
            #    "__builtins__": __builtins__,
            #    "__name__": "__config__",
            #    "__file__": config,
            #    "__doc__": None,
            #    "__package__": None
            #}
            cfg = {}
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
                        sys.stderr.write("Invalid value for %s: %s\n\n"\
                                          % (k, v))
                        raise
                    else:
                        if isfunction(v):
                            if v.__name__ in globals():
                                v = globals()[v.__name__]
                                self.cfg.set(k.lower(), v)
                            else:
                                globals()[v.__name__] = v
            
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
            
    def init(self, opts):
        pass
    
    def monitor_handler(self):
        '''Returns a application handler for the monitor.
By default it returns ``None``.'''
        return None
    
    # MONITOR AND WORKER CALLBACKS
    def on_info(self, worker, data):
        return data
    
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
    
    def on_actor_message(self, message):
        pass
        
    def on_actor_message_processed(self, message, result):
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
        if self.cfg.debug:
            self.loglevel = logging.DEBUG
        else:
            self.loglevel = self.cfg.loglevel
        config = config or self.cfg.logconfig
        super(Application,self).configure_logging(config = config)

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
                    
    def remote_functions(self):
        '''Provide with additional remote functions
to be added to the monitor dictionary of remote functions.

:rtype: a two dimensional tuple of remotes and actor_functions
    dictionaries.'''
        return None,None
    
    