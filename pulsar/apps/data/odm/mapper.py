from inspect import ismodule

from pulsar import EventHandler
from pulsar.utils.pep import native_str
from pulsar.utils.importer import import_module

from .query import AbstractQuery, Query, QueryError
from .transaction import Transaction, ModelDictionary
from .model import ModelType
from ..stores import create_store


class Manager(AbstractQuery):
    '''Used by the :class:`.Mapper` to link a data :class:`.Store` collection
    with a :class:`.Model`.

    For example::

        from pulsar.apps.data import odm

        class MyModel(odm.Model):
            group = odm.SymbolField()
            flag = odm.BooleanField()

        models = odm.Mapper()
        models.register(MyModel)

        manager = models[MyModel]

    A :class:`.Model` can specify a :ref:`custom manager <custom-manager>` by
    creating a :class:`Manager` subclass with additional methods::

        class MyModelManager(odm.Manager):

            def special_query(self, **kwargs):
                ...

    At this point we need to tell the model about the custom manager, and we do
    so by setting the ``manager_class`` class attribute in the
    :class:`.Model`::

        class MyModel(odm.Model):
            ...

            manager_class = MyModelManager

    .. attribute:: _model

        The :class:`.Model` associated with this manager

    .. attribute:: _store

        The :class:`.Store` associated with this manager

    .. attribute:: _mapper

        The :class:`.Mapper` where this :class:`.Manager` is registered
    '''
    query_class = Query

    def __init__(self, model, store=None, read_store=None, mapper=None):
        self._model = model
        self._store = store
        self._read_store = read_store or store
        self._mapper = mapper

    @property
    def _meta(self):
        return self._model._meta

    @property
    def _loop(self):
        return self._store._loop

    def __str__(self):
        if self._store:
            return '{0}({1} - {2})'.format(self.__class__.__name__,
                                           self._meta,
                                           self._store)
        else:
            return '{0}({1})'.format(self.__class__.__name__, self._meta)
    __repr__ = __str__

    def __call__(self, *args, **kwargs):
        '''Create a new model without commiting to database.
        '''
        return self._model(*args, **kwargs)

    def query(self):
        return self.query_class(self)

    def create_table(self):
        return self._store.create_table(self._model)

    #    QUERY IMPLEMENTATION
    def get(self, *args, **kw):
        if len(args) == 1:
            return self._read_store.get_model(self._model, args[0])
        elif args:
            raise QueryError("'get' expected at most 1 argument, %s given" %
                             len(args))
        else:
            qs = self.filter(**kw)
            return self._get(qs)
            raise NotImplementedError

    def filter(self, **kwargs):
        return self.query().filter(**kwargs)

    def exclude(self, **kwargs):
        return self.query().exclude(**kwargs)

    def union(self, *queries):
        return self.query().exclude(*queries)

    def intersect(self, *queries):
        return self.query().intersect(*queries)

    def where(self, *expressions):
        return self.query().where(*expressions)

    def count(self):
        return self.query().count()

    def all(self):
        return self.query().all()

    def begin(self):
        '''Begin a new :class:`.Transaction`.'''
        return self._mapper.begin()

    def new(self, *args, **kwargs):
        '''Create a new instance of :attr:`_model` and commit to server.
        '''
        with self._mapper.begin() as t:
            t.add(self._model(*args, **kwargs))
        return t.wait(self._get_instance)
    insert = new

    def save(self, instance):
        '''Save an existing ``instance`` of :attr:`_model`.
        '''
        with self._mapper.begin() as t:
            t.add(instance)
        return t.wait()
    update = save

    ##    INTERNAL
    def _get_instance(self, transaction):
        return transaction


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

    .. attribute:: pre_commit

        A signal which can be used to register ``callbacks`` before instances
        are committed::

            models.pre_commit.bind(callback, sender=MyModel)

    .. attribute:: pre_delete

        A signal which can be used to register ``callbacks`` before instances
        are deleted::

            models.pre_delete.bind(callback, sender=MyModel)

    .. attribute:: post_commit

        A signal which can be used to register ``callbacks`` after instances
        are committed::

            models.post_commit.bind(callback, sender=MyModel)

    .. attribute:: post_delete

        A signal which can be used to register ``callbacks`` after instances
        are deleted::

            models.post_delete.bind(callback, sender=MyModel)
    '''
    MANY_TIMES_EVENTS = ('pre_commit', 'pre_delete',
                         'post_commit', 'post_delete')

    def __init__(self, default_store, **kw):
        super(Mapper, self).__init__()
        self._registered_models = ModelDictionary()
        self._registered_names = {}
        self._default_store = create_store(default_store, **kw)
        self._loop = self._default_store._loop
        self._search_engine = None

    @property
    def default_store(self):
        '''The default :class:`.Store` for this :class:`Mapper`.

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
        '''The :class:`SearchEngine` for this :class:`Mapper`. This
must be created by users. Check :ref:`full text search <tutorial-search>`
tutorial for information.'''
        return self._search_engine

    def __repr__(self):
        return '%s %s' % (self.__class__.__name.__, self._registered_models)

    def __str__(self):
        return str(self._registered_models)

    def __contains__(self, model):
        return model in self._registered_models

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
        '''Set the search ``engine`` for this :class:`Mapper`.'''
        self._search_engine = engine
        self._search_engine.set_mapper(self)

    def register(self, model, store=None, read_store=None,
                 include_related=True, **params):
        '''Register a :class:`.Model` with this :class:`Mapper`.

        If the model was already registered it does nothing.

        :param model: a :class:`.Model` class.
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
        :return: the number of models registered.
        '''
        store = store or self._default_store
        store = create_store(store, **params)
        if read_store:
            read_store = create_store(read_store, *params)
        registered = 0
        for model in self.models_from_model(model,
                                            include_related=include_related):
            if model in self._registered_models:
                continue
            registered += 1
            default_manager = store.default_manager or Manager
            manager_class = getattr(model, 'manager_class', default_manager)
            manager = manager_class(model, store, read_store, self)
            self._registered_models[model] = manager
            if model._meta.name not in self._registered_names:
                self._registered_names[model._meta.name] = manager
        if registered:
            return store

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
            if include is not None and not (m.modelkey in include or
                                            m.app_label in include):
                continue
            if not (m.modelkey in exclude or m.app_label in exclude):
                if dryrun:
                    results.append(manager)
                else:
                    results.append(manager.flush())
        return results

    def unregister(self, model=None):
        '''Unregister a ``model`` if provided, otherwise it unregister all
registered models. Return a list of unregistered model managers or ``None``
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
        '''A higher level registration functions for group of models located
        on application modules.
        It uses the :func:`model_iterator` function to iterate
        through all :class:`.Model` models available in ``applications``
        and register them using the :func:`register` low level method.

        :parameter applications: A String or a list of strings representing
            python dotted paths where models are implemented.
        :parameter models: Optional list of models to include. If not provided
            all models found in *applications* will be included.
        :parameter stores: optional dictionary which map a model or an
            application to a store
            :ref:`connection string <connection-string>`.
        :rtype: A list of registered :class:`.Model`.

        For example::


            mapper.register_application_models('mylib.myapp')
            mapper.register_application_models(['mylib.myapp', 'another.path'])
            mapper.register_application_models(pythonmodule)
            mapper.register_application_models(['mylib.myapp',pythonmodule])

        '''
        return list(self._register_applications(applications, models,
                                                stores))

    def create_all(self):
        '''Loop though :attr:`registered_models` and issue the
        :meth:`Manager.create_all` method.'''
        for manager in self._registered_models.values():
            manager.create_all()

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
            return hasattr(model, '_meta')
        return False

    def models_from_model(self, model, include_related=False, exclude=None):
        '''Generator of all model in model.'''
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


class LazyProxy(object):
    '''Base class for lazy descriptors.

    .. attribute:: field

        The :class:`Field` which create this descriptor. Either a
        :class:`ForeignKey` or a :class:`StructureField`.
    '''
    def __init__(self, field):
        self.field = field

    def __repr__(self):
        return self.field.name
    __str__ = __repr__

    @property
    def name(self):
        return self.field.name

    def load(self, instance, session):
        '''Load the lazy data for this descriptor.'''
        raise NotImplementedError

    def load_from_manager(self, manager):
        raise NotImplementedError('cannot access %s from manager' % self)

    def __get__(self, instance, instance_type=None):
        if not self.field.class_field:
            if instance is None:
                return self
            return self.load(instance, instance.session)
        else:
            return self
