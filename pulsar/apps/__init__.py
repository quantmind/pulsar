"""
This module implements the main classes for pulsar application framework. The
framework is built on top of pulsar asynchronous engine and allows to
implement servers with very little effort. The main classes here are:
:class:`Application` and :class:`MultiApp` which, has the name suggests, is
a factory of several :class:`Application` running on a single server.
The :class:`Configurator` is a mixin used as base class for both
:class:`Application` and :class:`MultiApp`.


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

   .. automethod:: __call__


Multi App
===============================

.. autoclass:: MultiApp
   :members:
   :member-order: bysource


Get application
=========================

.. autofunction:: get_application


.. _when-monitor-start:

When monitor start
=================================

The application framework provides a way for adding hooks which are executed
every time a new application starts. A hook is registered by::

    from pulsar.apps import when_monitor_start

    def myhook(monitor):
        ...

    when_monitor_start.append(myhook)

By default, the list of hooks only contains a callback to start the
:ref:`default data store <setting-data_store>` if it needs to.
"""
import os
import sys
from inspect import getfile
from functools import partial
from collections import namedtuple, OrderedDict
import asyncio

import pulsar
from pulsar import get_actor, Config, task, Future, ImproperlyConfigured

__all__ = ['Application', 'MultiApp', 'get_application', 'when_monitor_start']


when_monitor_start = []
new_app = namedtuple('new_app', 'prefix params')


def get_application(name):
    """Fetch an :class:`Application` associated with ``name`` if available.

    This function may return an :ref:`asynchronous component <coroutine>`.
    The application name is set during initialisation. Check the
    :attr:`Configurator.name` attribute for more information.
    """
    actor = get_actor()

    if actor:
        if actor.is_arbiter():
            return _get_app(actor, name, False)
        else:
            return _get_remote_app(actor, name)


async def _get_remote_app(actor, name):
    cfg = await actor.send('arbiter', 'run', _get_app, name)
    return cfg.app() if cfg else None


async def _get_app(arbiter, name, safe=True):
    monitor = arbiter.get_actor(name)
    if monitor:
        cfg = await monitor.start_event
        if safe:
            return cfg
        else:
            return monitor.app


@task
async def monitor_start(self, exc=None):
    start_event = self.start_event
    if exc:
        start_event.set_exception(exc)
        return
    app = self.app
    try:
        self.bind_event('on_params', monitor_params)
        self.bind_event('on_info', monitor_info)
        self.bind_event('stopping', monitor_stopping)
        for callback in when_monitor_start:
            coro = callback(self)
            if coro:
                await coro
        self.bind_event('periodic_task', app.monitor_task)
        coro = app.monitor_start(self)
        if coro:
            await coro
        if not self.cfg.workers:
            coro = app.worker_start(self)
            if coro:
                await coro
        result = self.cfg
    except Exception as exc:
        coro = self.stop(exc)
        if coro:
            await coro
        start_event.set_result(None)
    else:
        start_event.set_result(result)


@task
async def monitor_stopping(self, exc=None):
    if not self.cfg.workers:
        coro = self.app.worker_stopping(self)
        if coro:
            await coro
    coro = self.app.monitor_stopping(self)
    if coro:
        await coro
    return self


def monitor_info(self, info=None):
    if not self.cfg.workers:
        self.app.worker_info(self, info)
    else:
        self.app.monitor_info(self, info)


def monitor_params(self, params=None):
    app = self.app
    params.update({'cfg': app.cfg.clone(),
                   'name': '%s.worker' % app.name,
                   'start': worker_start})
    app.actorparams(self, params)


def worker_start(self, exc=None):
    app = getattr(self, 'app', None)
    if app is None:
        cfg = self.cfg
        self.app = app = cfg.application.from_config(cfg, logger=self.logger)
    self.bind_event('on_info', app.worker_info)
    self.bind_event('stopping', app.worker_stopping)
    return app.worker_start(self, exc=exc)


class Configurator:
    """A mixin for configuring and loading a pulsar application server.

    :parameter name: to override the class :attr:`name` attribute.
    :parameter description: to override the class :attr:`cfg.description`
        attribute.
    :parameter epilog: to override the class :attr:`cfg.epilog` attribute.
    :parameter version: Optional version of this application, it overrides
        the class :attr:`cfg.version` attribute.
    :parameter argv: Optional list of command line parameters to parse, if
        not supplied the :attr:`sys.argv` list will be used. The parameter
        is only relevant if ``parse_console`` is ``True``.
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

        The :class:`.Config` for this :class:`Configurator`.
        If set as class attribute it will be replaced during initialisation.

        Default: ``None``.

    .. attribute:: console_parsed

        ``True`` if this application parsed the console before starting.

    .. attribute:: script

        Full path of the script which starts the application or ``None``.
        Evaluated during initialization via the :meth:`python_path` method.
    """
    argv = None
    name = None
    cfg = Config()

    def __init__(self,
                 name=None,
                 description=None,
                 epilog=None,
                 version=None,
                 argv=None,
                 parse_console=True,
                 script=None,
                 cfg=None,
                 **params):
        cls = self.__class__
        self.name = name or cls.name or cls.__name__.lower()
        if not isinstance(cfg, Config):
            cfg = cfg or {}
            cfg.update(params)
            cfg = cls.create_config(cfg)
        else:
            cfg.update(params)
        self.cfg = cfg
        cfg.description = description or self.cfg.description
        cfg.epilog = epilog or self.cfg.epilog
        cfg.version = version or self.cfg.version
        cfg.name = self.name
        cfg.application = cls
        self.argv = argv
        self.console_parsed = parse_console
        self.cfg.script = self.script = self.python_path(script)

    @property
    def version(self):
        """Version of this :class:`Application`"""
        return self.cfg.version

    @property
    def root_dir(self):
        """Root directory of this :class:`Configurator`.

        Evaluated from the :attr:`script` attribute.
        """
        if self.cfg.script:
            return os.path.dirname(self.cfg.script)

    def __repr__(self):
        return self.name

    def __str__(self):
        return self.__repr__()

    def python_path(self, script):
        """Called during initialisation to obtain the ``script`` name.

        If ``script`` does not evaluate to ``True`` it is evaluated from
        the ``__main__`` import. Returns the real path of the python
        script which runs the application.
        """
        if not script:
            try:
                import __main__
                script = getfile(__main__)
            except Exception:  # pragma    nocover
                return
        script = os.path.realpath(script)
        if self.cfg.get('python_path', True):
            path = os.path.dirname(script)
            if path not in sys.path:
                sys.path.insert(0, path)
        return script

    def on_config(self, arbiter):
        """Callback when configuration is loaded.

        This is a chance to do applications specific checks before the
        concurrent machinery is put into place.

        If it returns ``False`` the application will abort.
        """
        pass

    def load_config(self):
        """Load the application configuration from a file and/or
        from the command line.

        Called during application initialisation. The parameters
        overriding order is the following:

        * default parameters.
        * the key-valued params passed in the initialisation.
        * the parameters in the optional configuration file
        * the parameters passed in the command line.
        """
        # get the actor if available and override default cfg values with those
        # from the actor
        actor = get_actor()
        if actor and actor.is_running():
            # actor available and running.
            # Unless argv is set, skip parsing
            if self.argv is None:
                self.console_parsed = False
            # copy global settings
            self.cfg.copy_globals(actor.cfg)
        #
        for name in list(self.cfg.params):
            if name in self.cfg.settings:
                value = self.cfg.params.pop(name)
                if value is not None:
                    self.cfg.set(name, value)
        # parse console args
        if self.console_parsed:
            self.cfg.parse_command_line(self.argv)
        else:
            self.cfg.params.update(self.cfg.import_from_module())

    def start(self, exit=True):
        """Invoked the application callable method and start
        the ``arbiter`` if it wasn't already started.

        It returns a :class:`~asyncio.Future` called back once the
        application/applications are running. It returns ``None`` if
        called more than once.
        """
        on_start = self()
        arbiter = pulsar.arbiter()
        if arbiter and on_start:
            arbiter.start(exit=exit)
            if arbiter.exit_code is not None:
                return arbiter.exit_code
        return on_start

    @classmethod
    def create_config(cls, params, prefix=None, name=None):
        """Create a new :class:`.Config` container.

        Invoked during initialisation, it overrides defaults
        with ``params`` and apply the ``prefix`` to non global
        settings.
        """
        if isinstance(cls.cfg, Config):
            cfg = cls.cfg.copy(name=name, prefix=prefix)
        else:
            cfg = cls.cfg.copy()
            if name:
                cfg[name] = name
            if prefix:
                cfg[prefix] = prefix
            cfg = Config(**cfg)
        cfg.update_settings()
        cfg.update(params, True)
        return cfg


class Application(Configurator):
    """An application interface.

    Applications can be of any sorts or forms and the library is shipped with
    several battery included examples in the :mod:`pulsar.apps` module.

    These are the most important facts about a pulsar :class:`Application`:

    * It derives from :class:`Configurator` so that it has all the
      functionalities to parse command line arguments and setup the
      :attr:`~Configurator.cfg` placeholder of :class:`.Setting`.
    * Instances of an :class:`Application` are callable objects accepting
      the calling actor as only argument. The callable method should
      not be overwritten, instead one should overwrites the application
      hooks available.
    * When an :class:`Application` is called for the first time,
      a new ``monitor`` is added to the ``arbiter``,
      ready to perform its duties.

    :parameter callable: Initialise the :attr:`callable` attribute.
    :parameter load_config: If ``False`` the :meth:`~Configurator.load_config`
        method is not invoked.

        Default ``True``.
    :parameter params: Passed to the :class:`Configurator` initialiser.

    .. attribute:: callable

        Optional callable serving or configuring your application.
        If provided, the callable must be picklable, therefore it is either
        a function or a picklable object.

        Default ``None``
    """
    def __init__(self, callable=None, load_config=True, **params):
        super().__init__(**params)
        self.cfg.callable = callable
        self.logger = None
        if load_config:
            self.load_config()

    @classmethod
    def from_config(cls, cfg, logger=None):
        c = cls.__new__(cls)
        c.name = cfg.name
        c.cfg = cfg
        c.logger = logger or cfg.configured_logger()
        return c

    @property
    def stream(self):
        """Actor stream handler.
        """
        return get_actor().stream

    def __call__(self, actor=None):
        """Register this application with the (optional) calling ``actor``.

        If an ``actor`` is available (either via the function argument or via
        the :func:`~pulsar.async.actor.get_actor` function) it must be
        ``arbiter``, otherwise this call is no-op.

        If no actor is available, it means this application starts
        pulsar engine by creating the ``arbiter`` with its
        :ref:`global settings <setting-section-global-server-settings>`
        copied to the arbiter :class:`.Config` container.

        :return: the ``start`` one time event fired once this application
            has fired it.
        """
        if actor is None:
            actor = get_actor()
        monitor = None
        if actor and actor.is_arbiter():
            monitor = actor.get_actor(self.name)
        if monitor is None and (not actor or actor.is_arbiter()):
            self.cfg.on_start()
            self.logger = self.cfg.configured_logger()
            if not actor:
                actor = pulsar.arbiter(cfg=self.cfg.clone())
            else:
                self.update_arbiter_params(actor)
            if not self.cfg.exc_id:
                self.cfg.set('exc_id', actor.cfg.exc_id)
            if self.on_config(actor) is not False:
                start = Future(loop=actor._loop)
                actor.bind_event('start', partial(self._add_monitor, start))
                return start
            else:
                return
        elif monitor:
            raise ImproperlyConfigured('%s already started ' % monitor.name)
        else:
            raise ImproperlyConfigured('Cannot start application from %s'
                                       % actor)

    def stop(self, actor=None):
        """Stop the application
        """
        if actor is None:
            actor = get_actor()
        if actor and actor.is_arbiter():
            monitor = actor.get_actor(self.name)
            if monitor:
                return monitor.stop()
        raise RuntimeError('Cannot stop application')

    # WORKERS CALLBACKS
    def worker_start(self, worker, exc=None):
        """Added to the ``start`` :ref:`worker hook <actor-hooks>`."""
        pass

    def worker_info(self, worker, info):
        """Hook to add additional entries to the worker ``info`` dictionary.
        """
        pass

    def worker_stopping(self, worker, exc=None):
        """Added to the ``stopping`` :ref:`worker hook <actor-hooks>`."""
        pass

    # MONITOR CALLBACKS
    def actorparams(self, monitor, params=None):
        """Hook to add additional entries when the monitor spawn new actors.
        """
        pass

    def monitor_start(self, monitor):
        """Callback by the monitor when starting.
        """
        pass

    def monitor_info(self, monitor, info):
        """Hook to add additional entries to the monitor ``info`` dictionary.
        """
        pass

    def monitor_stopping(self, monitor):
        """Callback by the monitor before stopping.
        """
        pass

    def monitor_task(self, monitor):
        """Executed by the :class:`.Monitor` serving this application
        at each event loop."""
        pass

    def update_arbiter_params(self, arbiter):
        for s in self.cfg.settings.values():
            if s.is_global and s.modified:
                a = arbiter.cfg.settings[s.name]
                if not a.modified:
                    a.set(s.value)

    #   INTERNALS
    def _add_monitor(self, start, arbiter, exc=None):
        if not exc:
            monitor = arbiter.add_monitor(
                self.name, app=self, cfg=self.cfg,
                start=monitor_start, start_event=start)
            self.cfg = monitor.cfg


class MultiApp(Configurator):
    """A :class:`MultiApp` is a tool for creating several :class:`Application`
    and starting them at once.

    It makes sure all :ref:`settings <settings>` for the
    applications created are available in the command line.

    :class:`MultiApp` derives from :class:`Configurator` and therefore
    supports all its configuration utilities,
    :meth:`build` is the only method which must be implemented by
    subclasses.

    A minimal example usage::

        import pulsar

        class Server(pulsar.MultiApp):

            def build(self):
                yield self.new_app(TaskQueue)
                yield self.new_app(WSGIserver, "rpc", callable=..., ...)
                yield self.new_app(WSGIserver, "web", callable=..., ...)
    """
    _apps = None

    def build(self):
        """Virtual method, must be implemented by subclasses and return an
        iterable over results obtained from calls to the
        :meth:`new_app` method.
        """
        raise NotImplementedError

    def apps(self):
        """List of :class:`Application` for this :class:`MultiApp`.

        The list is lazily loaded from the :meth:`build` method.
        """
        if self._apps is None:
            # Add modified settings values to the list of cfg params
            self.cfg.params.update(((s.name, s.value) for s in
                                    self.cfg.settings.values() if s.modified))
            self.cfg.settings = {}
            self._apps = OrderedDict()
            self._apps.update(self._build())
            if not self._apps:
                return []
            # Load the configuration (command line and config file)
            self.load_config()
            kwargs = self._get_app_params()
            apps = self._apps
            self._apps = []
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
                self._apps.append(App(**params))
        return self._apps

    def new_app(self, App, prefix=None, callable=None, **params):
        """Invoke this method in the :meth:`build` method as many times
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
        """
        params.update(self.cfg.params)
        params.pop('name', None)    # remove the name
        prefix = prefix or ''
        if not prefix and '' in self._apps:
            prefix = App.name or App.__name__.lower()
        if not prefix:
            name = self.name
            cfg = App.create_config(params, name=name)
        else:
            name = '%s_%s' % (prefix, self.name)
            cfg = App.create_config(params, prefix=prefix, name=name)
        # Add the config entry to the multi app config if not available
        for k in cfg.settings:
            if k not in self.cfg.settings:
                self.cfg.settings[k] = cfg.settings[k]
        return new_app(prefix, (App, name, callable, cfg))

    def __call__(self, actor=None):
        apps = [app(actor) for app in self.apps()]
        return asyncio.gather(*apps, loop=get_actor()._loop)

    #    INTERNALS
    def _build(self):
        for app in self.build():
            if not isinstance(app, new_app):
                raise ImproperlyConfigured(
                    'You must use new_app when building a MultiApp')
            yield app

    def _iter_app(self, app_name_callables):
        main = app_name_callables.pop('', None)
        if not main:
            raise ImproperlyConfigured('No main application in MultiApp')
        yield main
        for app in app_name_callables.values():
            yield app

    def _get_app_params(self):
        params = self.cfg.params.copy()
        for key, value in self.__dict__.items():
            if key.startswith('_'):
                continue
            elif key == 'console_parsed':
                params['parse_console'] = not value
            else:
                params[key] = value
        params['load_config'] = False
        return params
