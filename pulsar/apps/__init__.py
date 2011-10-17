import logging
import os
import sys
import traceback
import random
from inspect import isgenerator

import pulsar
from pulsar import Empty, make_async, is_failure, async_pair, Failure
from pulsar.utils.py2py3 import execfile
from pulsar.utils.importer import import_module
from pulsar.utils.log import LogInformation
from pulsar.utils import system
#from pulsar.utils import debug

__all__ = ['Worker',
           'Application',
           'ApplicationMonitor',
           'WorkerRequest',
           'Response',
           'require',
           'ResponseError']


def require(appname):
    '''Shortcut function to load an application'''
    apps = appname.split('.')
    if len(apps) == 1:
        module = 'pulsar.apps.{0}'.format(appname)
    else:
        module = appname
    mod = import_module(module)
    return mod


class WorkerRequest(object):
    
    def response(self):
        return self
    
    def close(self):
        pass
    
    
class Response(object):
    '''A mixin for pulsar response classes'''
    exception = None
    def __init__(self, request):
        self.request = request
        
    def close(self):
        pass


class ResponseError(pulsar.PulsarException,Response):
    
    def __init__(self, request, failure):
        pulsar.Response.__init__(self, request)
        self.exception = Failure(failure)
    
    def close(self):
        return self.request.close()
        
        
def make_response(request, response, err = None):
    if is_failure(response):
        response = ResponseError(request,response)
    if err:
        if not hasattr(response,'exception'):
            response = ResponseError(request,err)
        else:
            response.exception = err.append(response.exception)
    return response



class HandlerMixin(object):
      
    def _response_generator(self, request):
        try:
            self.cfg.pre_request(self, request)
        except Exception:
            pass
        try:
            response = self.app.handle_request(self, request)
        except:
            response = ResponseError(request,sys.exc_info())
        
        response, outcome = async_pair(response)
        yield response
        response = make_response(request, outcome.result)
        yield response.close()
        yield response
        
    def handle_message(self, sender, message):
        '''Handle a *message* from a *sender*.'''
        return self.app.handle_message(sender, self, message)
    
    def handle_request(self, request):
        '''Entry point for handling a request. This is a high level
function which performs some pre-processing of *request* and delegates
the actual implementation to :meth:`Application.handle_request` method.

:parameter request: A request instance which is application specific.

After obtaining the result from the
:meth:`Application.handle_event_task` method, it invokes the
:meth:`Worker.end_task` method to close the request.'''
        self.nr += 1
        should_stop = self.max_requests and self.nr >= self.max_requests        
        make_async(self._response_generator(request)).add_callback(
               lambda res : self.close_response(request,res,should_stop))\
               .start(self.ioloop)
        
    def close_response(self, request, response, should_stop):
        '''Close the response. This method should be called by the
:meth:`Application.handle_response` once done.'''
        response = make_response(request, response)
        
        if response and response.exception:
            response.exception.log(self.log)
                        
        try:
            self.cfg.post_request(self, request)
        except:
            pass
        
        if should_stop:
            self.log.info("Auto-restarting worker.")
            self.stop()


class Worker(HandlerMixin,pulsar.Actor):
    """\
Base class for a :class:`Actor` serving a :class:`Application`.
It provides two new functions :meth:`handle_request` and :meth:`handle_response`
used for by the application for handling requests and sending back responses.
    
.. attribute:: app

    Instance of the :class:`Application` to be performed by the worker
    
.. attribute:: cfg

    Configuration dictionary

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
        if self.information.log():
            self.log.info('Processed {0} requests'.format(self.nr))
        self.app.worker_task(self)
    
    def on_stop(self):
        self.app.worker_stop(self)
            
    def on_exit(self):
        self.app.worker_exit(self)
        try:
            self.cfg.worker_exit(self)
        except:
            pass
        
    def on_info(self, data):
        data.update({'request processed': self.nr,
                     'max requests':self.cfg.max_requests})
        return data
    
    def configure_logging(self, **kwargs):
        #switch off configure logging. Done by self.app
        pass


class ApplicationMonitor(HandlerMixin,pulsar.Monitor):
    '''A spcialized :class:`Monitor` implementation for managing
pulsar applications (subclasses of :class:`Application`).
'''
    def on_init(self, app = None, **kwargs):
        self.app = app
        self.cfg = app.cfg
        self.max_requests = 0
        kwargs['actor_class'] = app.cfg.worker_class
        kwargs['num_actors'] = app.cfg.workers
        arbiter = pulsar.arbiter()
        if not arbiter.get('cfg'):
            arbiter['cfg'] = app.cfg
        self.app_handler = app.handler()
        super(ApplicationMonitor,self).on_init(**kwargs)
    
    # Delegates Callbacks to the application    
    def on_start(self):
        self.app.monitor_start(self)
        
    def monitor_task(self):
        self.app.monitor_task(self)
            
    def on_stop(self):
        stp = make_async(self.app.monitor_stop(self))
        return stp.add_callback(super(ApplicationMonitor,self).on_stop())
        
    def on_exit(self):
        self.app.monitor_exit(self)
    
    def clean_up(self):
        self.worker_class.clean_arbiter_loop(self,self.ioloop)
            
    def actorparams(self):
        '''Override the :meth:`Monitor.actorparams` method to
updated actor parameters with information about the application.

:rtype: a dictionary of parameters to be passed to the
    spawn method when creating new actors.'''
        return  {'app':self.app,
                 'timeout': self.app.cfg.timeout,
                 'loglevel': self.app.loglevel,
                 'impl': self.app.cfg.concurrency,
                 'name':'{0}-worker'.format(self.app.name)}

    def configure_logging(self, **kwargs):
        self.app.configure_logging(**kwargs)
        self.loglevel = self.app.loglevel
        
    def _info(self, result = None):
        info = super(ApplicationMonitor,self)._info(result)
        info.update({'default_timeout': self.cfg.timeout})
        return info
    

class Application(pulsar.PickableMixin):
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
    
    Default ``None``
    
.. attribute:: callable

    A callable serving your application. The callable must be pickable,
    therefore it is either a function
    or a pickable object. If not provided, the application must
    implement the :meth:`handler` method.
    
    Default ``None``
    
.. attribute:: cfg

    dictionary of default configuration parameters.
    
    Default: ``{}``
    
.. attribute:: mid

    The unique id of the :class:`ApplicationMonitor` managing the
    application. Defined at runtime.
"""
    cfg = {}
    _name = None
    description = None
    epilog = None
    app = None
    config_options_include = None
    config_options_exclude = None
    monitor_class = ApplicationMonitor
    default_logging_level = logging.INFO
    
    def __init__(self,
                 callable = None,
                 name = None,
                 description = None,
                 epilog = None,
                 argv = None,
                 **params):
        self.python_path()
        self.description = description or self.description
        self.epilog = epilog or self.epilog
        self._name = name or self._name or self.__class__.__name__.lower()
        nparams = self.cfg.copy()
        nparams.update(params)
        self.callable = callable
        self.load_config(argv,**nparams)
        if self.on_config() is not False:
            arbiter = pulsar.arbiter(self.cfg.daemon)
            monitor = arbiter.add_monitor(self.monitor_class,
                                          self.name,
                                          app = self,
                                          ioqueue = self.get_ioqueue())
            self.mid = monitor.aid
            r,f = self.remote_functions()
            if r:
                monitor.remotes = monitor.remotes.copy()
                monitor.remotes.update(r)
                monitor.actor_functions = monitor.actor_functions.copy()
                monitor.actor_functions.update(f)
    
    @property
    def name(self):
        '''Application name, It is unique and defines the application.'''
        return self._name
    
    def handle_message(self, sender, receiver, message):
        '''Handle messages for the *receiver*.'''
        handler = getattr(self,'actor_' + message.action,None)
        if handler:
            return handler(sender, receiver, *message.args, **message.kwargs)
        else:
            receiver.log.error('Unknown action ' + message.action)
        
    def handle_request(self, worker, request):
        '''This is the main function which needs to be implemented
by actual applications. It is called by the *worker* to handle
a *request*.

:parameter worker: the :class:`Worker` handling the request.
:parameter request: an application specific request object.
:rtype: It can be a generator, a :class:`Deferred` instance
    or the actual response.'''
        raise NotImplementedError
    
    def get_ioqueue(self):
        '''Returns an I/O distributed queue for the application if one
is needed. If a queue is returned, the application :class:`Worker`
will have a :class:`IOLoop` instance based on the queue (via :class:`IOQueue`).
 
By default it returns ``None``.'''
        return None
    
    def on_config(self):
        '''Callback when configuration is loaded. This is a chanse to do
 an application specific check before the concurrent machinery is put into
 place. If it returns ``False`` the application will abort.'''
        pass
    
    def python_path(self):
        #Insert the application directory at the top of the python path.
        path = os.path.split(os.getcwd())[0]
        if path not in sys.path:
            sys.path.insert(0, path)
            
    def add_timeout(self, deadline, callback):
        self.arbiter.ioloop.add_timeout(deadline, callback)
              
    def load_config(self, argv, parse_console = True, **params):
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
        self.cfg = pulsar.Config(self.description,
                                 self.epilog,
                                 self.app,
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
                        sys.stderr.write("Invalid value for %s: %s\n\n"\
                                          % (k, v))
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
        names = name.split('__')
        if len(names) == 2 and names[0] == self.name:
            name = names[1].lower()
            if name in self.cfg.settings:
                overrides[name] = value
                return True
            
    def init(self, opts):
        pass
    
    def handler(self):
        '''Returns a callable application handler,
used by a :class:`Worker` to carry out its task. By default it
returns the :attr:`Application.callable`.'''
        return self.callable
    
    # MONITOR AND WORKER CALLBACKS
    
    def update_worker_paramaters(self, monitor, params):
        '''Called by the :class:`ApplicationMonitor` when
returning from the :meth:`ApplicationMonitor.actorparams`
and just before spawning a new worker for serving the application.

:parameter monitor: instance of the monitor serving the application.
:parameter params: the dictionary of parameters to updated (if needed).
:rtype: the updated dictionary of parameters.

This callback is a chance for the application to pass its own custom
parameters to the workers before it is created.
By default it returns *params* without
doing anything.'''
        return params
    
    def worker_start(self, worker):
        '''Called by the :class:`Worker` after fork'''
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
    
    def monitor_start(self, monitor):
        '''Callback by :class:`ApplicationMonitor` when starting'''
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
    