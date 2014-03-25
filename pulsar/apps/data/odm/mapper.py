from inspect import ismodule

from pulsar import EventHandler, multi_async, wait_complete
from pulsar.utils.pep import native_str
from pulsar.utils.importer import import_module

from .transaction import Transaction, ModelDictionary
from .model import ModelType
from .manager import Manager
from . import query
from ..store import create_store


class Mapper(EventHandler):
    '''A mapper is a mapping of :class:`.Model` to a :class:`.Manager`.

    The :class:`.Manager` are registered with a :class:`.Store`::

        from asyncstore import odm

        models = odm.Mapper(store)
        models.register(MyModel, ...)

        # dictionary Notation
        query = models[MyModel].query()

        # or dotted notation (lowercase)
        query = models.mymodel.query()

    The ``models`` instance in the above snippet can be set globally if
    one wishes to do so.

    A :class:`.Mapper` has four events:

    * ``pre_commit``: fired before instances are committed::

            models.bind_event('pre_commit', callback)

    * ``pre_delete``: fired before instances are deleted::

            models.bind_event('pre_delete', callback)

    * ``pre_commit``: fired after instances are committed::

            models.bind_event('post_commit', callback)

    * ``post_delete``: fired after instances are deleted::

            models.bind_event('post_delete', callback)
    '''
    MANY_TIMES_EVENTS = ('pre_commit', 'pre_delete',
                         'post_commit', 'post_delete')
    ModelNotFound = query.ModelNotFound

    def __init__(self, default_store, **kw):
        super(Mapper, self).__init__()
        self._registered_models = ModelDictionary()
        self._registered_names = {}
        self._default_store = create_store(default_store, **kw)
        self._loop = self._default_store._loop
        self._search_engine = None

    @property
    def default_store(self):
        '''The default :class:`.Store` for this :class:`.Mapper`.

        Used when calling the :meth:`register` method without explicitly
        passing a :class:`.Store`.
        '''
        return self._default_store

    @property
    def registered_models(self):
        '''List of registered :class:`.Model`.'''
        return list(self._registered_models)

    @property
    def search_engine(self):
        '''The :class:`.SearchEngine` for this :class:`.Mapper`.

        This must be created by users and intalled on a mapper via the
        :meth:`set_search_engine` method.
        Check :ref:`full text search <odm-search>`
        tutorial for information.
        '''
        return self._search_engine

    def __repr__(self):
        return '%s %s' % (self.__class__.__name__, self._registered_models)

    def __str__(self):
        return str(self._registered_models)

    def __contains__(self, model):
        return model in self._registered_models

    def __iter__(self):
        return iter(self._registered_models)

    def __getitem__(self, model):
        return self._registered_models[model]

    def __getattr__(self, name):
        if name in self._registered_names:
            return self._registered_names[name]
        raise AttributeError('No model named "%s"' % name)

    def begin(self):
        '''Begin a new :class:`.Transaction`
        '''
        return Transaction(self)

    def set_search_engine(self, engine):
        '''Set the :class:`.SearchEngine` for this :class:`.Mapper`.

        Check :ref:`full text search <odm-search>`
        tutorial for information.
        '''
        self._search_engine = engine
        if engine:
            self._search_engine.set_mapper(self)

    def register(self, *models, **params):
        '''Register one or several :class:`.Model` with this :class:`Mapper`.

        If a model was already registered it does nothing.

        :param models: a list of :class:`.Model`
        :param store: a :class:`.Store` or a connection string.
        :param read_store: Optional :class:`.Store` for read
            operations. This is useful when the server has a master/slave
            configuration, where the master accept write and read operations
            and the ``slave`` read only operations (Redis).
        :param include_related: ``True`` if related models to ``model``
            needs to be registered.
            Default ``True``.
        :param params: Additional parameters for the :func:`.create_store`
            function.
        :return: a list models registered or a single model if there
            was only one
        '''
        include_related = params.pop('include_related', True)
        store = params.pop('store', None) or self._default_store
        read_store = params.pop('read_store', None)
        store = create_store(store, **params)
        if read_store:
            read_store = create_store(read_store, *params)
        registered = []
        for model in models:
            for model in self.models_from_model(
                    model, include_related=include_related):
                if model in self._registered_models:
                    continue
                registered.append(model)
                default_manager = store.default_manager or Manager
                manager_class = getattr(model, 'manager_class',
                                        default_manager)
                manager = manager_class(model, store, read_store, self)
                self._registered_models[model] = manager
                if model._meta.name not in self._registered_names:
                    self._registered_names[model._meta.name] = manager
        return registered[0] if len(registered) == 1 else registered

    def from_uuid(self, uuid, session=None):
        '''Retrieve a :class:`.Model` from its universally unique identifier
``uuid``. If the ``uuid`` does not match any instance an exception will raise.
'''
        elems = uuid.split('.')
        if len(elems) == 2:
            model = get_model_from_hash(elems[0])
            if not model:
                raise Model.DoesNotExist(
                    'model id "{0}" not available'.format(elems[0]))
            if not session or session.mapper is not self:
                session = self.session()
            return session.query(model).get(id=elems[1])
        raise Model.DoesNotExist('uuid "{0}" not recognized'.format(uuid))

    def flush(self, exclude=None, include=None, dryrun=False):
        '''Flush :attr:`registered_models`.

        :param exclude: optional list of model names to exclude.
        :param include: optional list of model names to include.
        :param dryrun: Doesn't remove anything, simply collect managers
            to flush.
        :return:
        '''
        exclude = exclude or []
        results = []
        for manager in self._registered_models.values():
            m = manager._meta
            if include is not None and not (m.table_name in include or
                                            m.app_label in include):
                continue
            if not (m.table_name in exclude or m.app_label in exclude):
                if dryrun:
                    result = yield manager.query().count()
                else:
                    result = yield manager.query().delete()
                results.append((manager, result))
        coroutine_return(results)

    def unregister(self, model=None):
        '''Unregister a ``model`` if provided, otherwise it unregister all
        registered models.

        Return a list of unregistered model managers or ``None``
        if no managers were removed.'''
        if model is not None:
            try:
                manager = self._registered_models.pop(model)
            except KeyError:
                return
            if self._registered_names.get(manager._meta.name) == manager:
                self._registered_names.pop(manager._meta.name)
            return [manager]
        else:
            managers = list(self._registered_models.values())
            self._registered_models.clear()
            return managers

    def register_applications(self, applications, models=None, stores=None):
        '''A higher level registration method for group of models located
        on application modules.

        It uses the :meth:`model_iterator` method to iterate
        through all :class:`.Model` available in ``applications``
        and :meth:`register` them.

        :parameter applications: A String or a list of strings representing
            python dotted paths where models are implemented. Can also be
            a module or a list of modules.
        :parameter models: Optional list of models to include. If not provided
            all models found in *applications* will be included.
        :parameter stores: optional dictionary which map a model or an
            application to a store
            :ref:`connection string <connection-string>`.
        :rtype: A list of registered :class:`.Model`.

        For example::


            mapper.register_applications('mylib.myapp')
            mapper.register_applications(['mylib.myapp', 'another.path'])
            mapper.register_applications(pythonmodule)
            mapper.register_applications(['mylib.myapp', pythonmodule])

        '''
        return list(self._register_applications(applications, models,
                                                stores))

    @wait_complete
    def search(self, *kw):
        raise NotImplementedError

    @wait_complete
    def create_tables(self, remove_existing=False):
        '''Loop though :attr:`registered_models` and issue the
        :meth:`.Manager.create_table` method.'''
        executed = []
        search_engine = self.search_engine
        for manager in self._registered_models.values():
            executed.append(manager.create_table(remove_existing))
        return multi_async(executed, loop=self._loop)

    @wait_complete
    def drop_tables(self):
        '''Loop though :attr:`registered_models` and issue the
        :meth:`.Manager.drop_table` method.'''
        executed = []
        for manager in self._registered_models.values():
            executed.append(manager.drop_table())
        return multi_async(executed, loop=self._loop)

    # PRIVATE METHODS
    def _register_applications(self, applications, models, stores):
        stores = stores or {}
        for model in self.model_iterator(applications):
            name = str(model._meta)
            if models and name not in models:
                continue
            if name not in stores:
                name = model._meta.app_label
            kwargs = stores.get(name, self._default_store)
            if not isinstance(kwargs, dict):
                kwargs = {'backend': kwargs}
            else:
                kwargs = kwargs.copy()
            if self.register(model, include_related=False, **kwargs):
                yield model

    def valid_model(self, model):
        if isinstance(model, ModelType):
            return not model._meta.abstract
        return False

    def models_from_model(self, model, include_related=False, exclude=None):
        '''Generator of all model in model.

        :param model: a :class:`.Model`
        :param include_related: if ``True`` al related models to ``model``
            are included
        :param exclude: optional set of models to exclude
        '''
        if exclude is None:
            exclude = set()
        if self.valid_model(model) and model not in exclude:
            exclude.add(model)
            yield model
            if include_related:
                for column in model._meta.dfields.values():
                    for fk in column.foreign_keys:
                        for model in (fk.column.table,):
                            for m in self.models_from_model(
                                    model, include_related=include_related,
                                    exclude=exclude):
                                yield m

    def model_iterator(self, application, include_related=True, exclude=None):
        '''A generator of :class:`.Model` classes found in *application*.

        :parameter application: A python dotted path or an iterable over
            python dotted-paths where models are defined.

        Only models defined in these paths are considered.
        '''
        if exclude is None:
            exclude = set()
        application = native_str(application)
        if ismodule(application) or isinstance(application, str):
            if ismodule(application):
                mod, application = application, application.__name__
            else:
                try:
                    mod = import_module(application)
                except ImportError:
                    # the module is not there
                    mod = None
            if mod:
                label = application.split('.')[-1]
                try:
                    mod_models = import_module('.models', application)
                except ImportError:
                    mod_models = mod
                label = getattr(mod_models, 'app_label', label)
                models = set()
                for name in dir(mod_models):
                    value = getattr(mod_models, name)
                    for model in self.models_from_model(
                            value, include_related=include_related,
                            exclude=exclude):
                        if (model._meta.app_label == label
                                and model not in models):
                            models.add(model)
                            yield model
        else:
            for app in application:
                for m in self.model_iterator(app,
                                             include_related=include_related,
                                             exclude=exclude):
                    yield m
