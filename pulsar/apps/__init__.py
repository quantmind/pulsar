'''
This module implements the main classes for pulsar application framework. The
framework is built on top of pulsar asynchronous engine and allows to
implement servers with very little effort. The main classes here are:
:class:`Application` and :class:`MultiApp` which, has the name suggests, is
a factory of several :class:`Application` running on a single server.
The :class:`Configurator` is a mixin used as base class for both
:class:`Application` and :class:`MultiApp`.

.. note::

    An instance of an :class:`Application` is picklable and therefore can be
    sent from actor to actor using the
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

'''
import os
import sys
from hashlib import sha1
from inspect import getfile

import pulsar
from pulsar import get_actor, EventHandler, coroutine_return
from pulsar.utils.structures import OrderedDict
from pulsar.utils.pep import pickle
from pulsar.utils.internet import (parse_connection_string,
                                   get_connection_string)
from pulsar.utils.log import LocalMixin, local_property
from pulsar.utils.importer import import_module

__all__ = ['Application', 'MultiApp', 'get_application']


def get_application(name):
    '''Fetch the :class:`Application` associated with ``name`` if available.

This function may return an :ref:`asynchronous component <coroutine>`.
The application name is set during initialisation. Check the
:attr:`Configurator.name` attribute for more information.
'''
    actor = get_actor()
    if actor:
        if actor.is_arbiter():
            return _get_app(actor, name)
        else:
            return actor.send('arbiter', 'run', _get_app, name)


def _get_app(arbiter, name):
    monitor = arbiter.get_actor(name)
    if monitor:
        return monitor.params.app


def monitor_start(self):
    app = self.params.app
    try:
        self.app = app
        self.bind_event('on_params', monitor_params)
        self.bind_event('on_info', monitor_info)
        self.bind_event('stopping', monitor_stopping)
        self.bind_event('stop', monitor_stop)
        self.monitor_task = lambda: app.monitor_task(self)
        yield app.monitor_start(self)
        if not self.cfg.workers:
            yield app.worker_start(self)
        result = app
    except Exception:
        result = sys.exc_info()
        raise
    finally:
        yield app.fire_event('start', result)
    coroutine_return(self)


def monitor_stopping(self):
    if not self.cfg.workers:
        yield self.app.worker_stopping(self)
    yield self.app.monitor_stopping(self)
    coroutine_return(self)


def monitor_stop(self):
    try:
        if not self.cfg.workers:
            yield self.app.worker_stop(self)
        yield self.app.monitor_stop(self)
    finally:
        yield self.app.fire_event('stop')
    coroutine_return(self)


def monitor_info(self, info=None):
    if not self.cfg.workers:
        self.app.worker_info(self, info)
    else:
        self.app.monitor_info(self, info)


def monitor_params(self, params=None):
    app = self.app
    params.update({'app': pickle.loads(pickle.dumps(app)),
                   'name': '{0}-worker'.format(app.name),
                   'start': worker_start})
    app.actorparams(self, params)


def worker_start(self):
    app = self.params.app
    self.app = app
    self.bind_event('on_info', app.worker_info)
    self.bind_event('stopping', app.worker_stopping)
    self.bind_event('stop', app.worker_stop)
    return app.worker_start(self)


def arbiter_config(cfg):
    cfg = cfg.copy()
    for setting in list(cfg.settings.values()):
        if setting.app:  # Don't include application specific settings
            cfg.settings.pop(setting.name)
        elif not setting.is_global:
            if not getattr(setting, 'inherit', False):
                setting.set(setting.default)
    return cfg


class Configurator(object):
    '''A mixin for configuring and loading a pulsar application server.

    :parameter name: to override the class :attr:`name` attribute.
    :parameter description: to override the class :attr:`cfg.description`
        attribute.
    :parameter epilog: to override the class :attr:`cfg.epilog` attribute.
    :parameter version: Optional version of this application, it overrides
        the class :attr:`cfg.version` attribute.
    :parameter argv: Optional list of command line parameters to parse, if
        not supplied the :attr:`sys.argv` list will be used. The parameter
        is only relevant if **parse_console** is ``True``.
    :parameter parse_console: ``True`` (default) if the console parameters
        needs parsing.
    :parameter script: Optional string which set the :attr:`script`
        attribute.
    :parameter params: a dictionary of configuration parameters which
        overrides the defaults and the :attr:`cfg` class attribute.
        They will be overwritten by a :ref:`config file <setting-config>`
        or command line arguments.

    .. attribute:: name

        The name is unique if this is an :class:`Application`. In this
        case it defines the application monitor name as well and can be
        access in the arbiter domain via the :func:`get_application`
        function.

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
                 **params):
        cls = self.__class__
        self.name = name or cls.name or cls.__name__.lower()
        if load_config or not isinstance(cfg, pulsar.Config):
            cfg = cfg or {}
            cfg.update(params)
            cfg = cls.create_config(cfg)
        self.cfg = cfg
        self.cfg.description = description or self.cfg.description
        self.cfg.epilog = epilog or self.cfg.epilog
        self.cfg.version = version or self.cfg.version
        self.cfg.name = self.name
        self.argv = argv
        self.parsed_console = parse_console
        self.script = self.python_path(script)

    @property
    def version(self):
        '''Version of this :class:`Application`'''
        return self.cfg.version

    @property
    def root_dir(self):
        '''Root directory of this :class:`Configurator`.

        Evaluated from the :attr:`script` attribute.
        '''
        if self.script:
            return os.path.dirname(self.script)

    def __repr__(self):
        return self.name

    def __str__(self):
        return self.__repr__()

    def python_path(self, script):
        '''Called during initialisation to obtain the ``script`` name and
        to add the :attr:`script` directory to the python path if not in the
        path already.
        If ``script`` does not evalueate to ``True`` it is evaluated from
        the ``__main__`` import. Returns the real path of the python
        script which runs the application.
        '''
        if not script:
            try:
                import __main__
            except ImportError:  # pragma    nocover
                return
            script = getfile(__main__)
        script = os.path.realpath(script)
        path = os.path.dirname(script)
        if path not in sys.path:
            sys.path.insert(0, path)
        return script

    def on_config(self, arbiter):
        '''Callback when configuration is loaded.

        This is a chance to do applications specific checks before the
        concurrent machinery is put into place.

        If it returns ``False`` the application will abort.
        '''
        pass

    def load_config(self):
        '''Load the application configuration from a file and/or
        from the command line.

        Called during application initialisation. The parameters
        overriding order is the following:

         * default parameters.
         * the *params* passed in the initialisation.
         * the parameters in the optional configuration file
         * the parameters passed in the command line.
        '''
        # get the actor if available and override default cfg values with those
        # from the actor
        actor = get_actor()
        cfg = self.cfg
        if actor and actor.is_running():
            # actor available and running. unless argv is set, skip parsing
            if self.argv is None:
                self.parsed_console = False
            # copy global settings
            self.cfg.copy_globals(actor.cfg)
        #
        for name in list(self.cfg.params):
            if name in self.cfg.settings:
                value = self.cfg.params.pop(name)
                if value is not None:
                    self.cfg.set(name, value)
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
    def create_config(cls, params, prefix=None, name=None):
        '''Create a new :class:`pulsar.utils.config.Config` container.

        Overrides defaults with ``params``.'''
        if cls.cfg:
            cfg = cls.cfg.copy(name=name, prefix=prefix)
            # update with latest settings
            cfg.update_settings()
            cfg.update(params)
        else:
            cfg = pulsar.Config(name=name, prefix=prefix, **params)
        return cfg


class Application(Configurator, pulsar.Pulsar):
    """An application interface.

    Applications can be of any sorts or forms and the library is shipped with
    several battery included examples in the :mod:`pulsar.apps` framework
    module.

    These are the most important facts about a pulsar :class:`Application`:

    * It derives from :class:`Configurator` so that it has all the
      functionalities to parse command line arguments and setup the
      :attr:`Configurator.cfg`.
    * Instances must be picklable. If non-picklable data needs to be added
      on an :class:`Application` instance, it should be stored on the
      ``local`` attribute dictionary (:class:`Application` derives
      from :class:`pulsar.utils.log.LocalMixin`).
    * Instances of an :class:`Application` are callable objects accepting
      the calling actor as only argument.
    * When an :class:`Application` is called for the first time,
      a new :class:`pulsar.Monitor` instance is added to the
      :class:`pulsar.Arbiter`, ready to perform its duties.

    :parameter callable: Initialise the :attr:`Application.callable` attribute.
    :parameter load_config: If ``False`` the :meth:`Configurator.load_config`
        is not invoked. Default ``True``.
    :parameter params: Passed to the :class:`Configurator` initialiser.

    .. attribute:: callable

        Optional callable serving or configuring your application.
        If provided, the callable must be picklable, therefore it is either
        a function or a picklable object.

        Default ``None``
    """
    def __init__(self, callable=None, load_config=True, **params):
        super(Application, self).__init__(load_config=load_config, **params)
        self.callable = callable
        if load_config:
            self.load_config()

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
            arbiter = pulsar.arbiter(cfg=arbiter_config(self.cfg))
            if self.on_config(arbiter) is not False:
                if arbiter.started():
                    self._add_to_arbiter(arbiter)
                else:   # the arbiter has not yet started.
                    arbiter.bind_event('start', self._add_to_arbiter)
            else:
                return
        return self.event('start')

    @local_property
    def events(self):
        '''Events for the application: ``ready```, ``start``, ``stop``.'''
        return EventHandler(one_time_events=('ready', 'start', 'stop'))

    @property
    def stream(self):
        '''Actor stream handler.
        '''
        return get_actor().stream

    def fire_event(self, name, *args, **kw):
        '''Fire event ``name``.'''
        if len(args) > 2:
            raise TypeError('fire_event takes at most 1 argument (%s given)'
                            % len(args))
        arg = args[0] if args else self
        return self.events.fire_event(name, arg, **kw)

    def bind_event(self, name, callback):
        '''Bind ``callback`` to event ``name``.'''
        return self.events.bind_event(name, callback)

    def event(self, name):
        '''Return the event ``name``.'''
        return self.events.event(name)

    # WORKERS CALLBACKS
    def worker_start(self, worker):
        '''Added to the ``start`` :ref:`worker hook <actor-hooks>`.'''
        pass

    def worker_info(self, worker, info):
        '''Hook to add additional entries to the worker ``info`` dictionary.
        '''
        pass

    def worker_stopping(self, worker):
        '''Added to the ``stopping`` :ref:`worker hook <actor-hooks>`.'''
        pass

    def worker_stop(self, worker):
        '''Added to the ``stop`` :ref:`worker hook <actor-hooks>`.'''
        pass

    # MONITOR CALLBACKS
    def actorparams(self, monitor, params=None):
        '''Hook to add additional entries when the monitor spawn new actors.
        '''
        pass

    def monitor_start(self, monitor):
        '''Callback by the monitor when starting.
        '''
        pass

    def monitor_info(self, monitor, info):
        '''Hook to add additional entries to the monitor ``info`` dictionary.
        '''
        pass

    def monitor_stopping(self, monitor):
        '''Callback by the monitor before stopping.
        '''
        pass

    def monitor_stop(self, monitor):
        '''Callback by the monitor on stop.
        '''
        pass

    def monitor_task(self, monitor):
        '''Callback by the monitor at each event loop.'''
        pass

    def start(self):
        '''Start the :class:`pulsar.Arbiter` if it wasn't already started.

        Calling this method when the :class:`pulsar.Arbiter` is already
        running has no effect.
        '''
        on_start = self()
        arbiter = pulsar.arbiter()
        if arbiter and on_start:
            arbiter.start()
        return self

    #   INTERNALS
    def _add_to_arbiter(self, arbiter):
        monitor = arbiter.add_monitor(
            self.name, app=self, cfg=self.cfg, start=monitor_start)
        self.cfg = monitor.cfg
        return arbiter


class MultiApp(Configurator):
    '''A :class:`MultiApp` is a tool for creating several :class:`Application`
and starting them at once. It makes sure all :ref:`settings <settings>` for the
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
            yield self.new_app(WSGIserver, prefix="rpc", callable=..., ...)
            yield self.new_app(WSGIserver, prefix="web", callable=..., ...)
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
            self.cfg.params.update(((s.name, s.value) for s in
                                    self.cfg.settings.values() if
                                    s.value != s.default))
            self.cfg.settings = {}
            self._apps = []
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
                if name == self.name:
                    params = kwargs.copy()
                    params['version'] = self.version
                else:
                    params = kwargs
                app = App(**params)
                app()
                self._apps.append(app)
        return self._apps

    def new_app(self, App, prefix=None, callable=None, **params):
        '''Invoke this method in the :meth:`build` method as many times
        as the number of :class:`Application` required by this
        :class:`MultiApp`.

        :param App: an :class:`Application` class.
        :param prefix: The prefix to use for the application,
            the prefix is appended to
            the application :ref:`config parameters <settings>` and to the
            application name. Each call to this methjod must use a different
            value of for this parameter. It can be ``None``.
        :param callable: optional callable (function of object) used during
            initialisation of *App* (the :class:`Application.callable`).
        :param params: additional key-valued parameters used when creating
            an instance of *App*.
        :return: a tuple used by the :meth:`apps` method.
        '''
        params.update(self.cfg.params.copy())
        params.pop('name', None)    # remove the name
        prefix = prefix or ''
        if not prefix:
            name = self.name
            cfg = App.create_config(params, name=name)
        else:
            name = '%s_%s' % (prefix, self.name)
            cfg = App.create_config(params, prefix=prefix, name=name)
        for k in cfg.settings:
            if k not in self.cfg.settings:
                self.cfg.settings[k] = cfg.settings[k]
        return prefix, (App, name, callable, cfg)

    def __call__(self, actor=None):
        return pulsar.multi_async((app(actor) for app in self.apps()))

    def start(self):
        '''Use this method to start all applications at once.'''
        self.apps()
        arbiter = pulsar.arbiter()
        if arbiter:
            arbiter.start()

    ##    INTERNALS
    def _iter_app(self, app_name_callables):
        main = app_name_callables.pop('', None)
        if not main:
            raise pulsar.ImproperlyConfigured(
                'No main application in MultiApp')
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
