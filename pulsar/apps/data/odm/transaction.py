from pulsar import EventHandler, coroutine_return, in_loop, InvalidOperation
from pulsar.utils.pep import iteritems

from .model import Model

from ..stores import Command


class ModelDictionary(dict):

    def __contains__(self, model):
        return super(ModelDictionary, self).__contains__(self.meta(model))

    def __getitem__(self, model):
        return super(ModelDictionary, self).__getitem__(self.meta(model))

    def __setitem__(self, model, value):
        super(ModelDictionary, self).__setitem__(self.meta(model), value)

    def get(self, model, default=None):
        return super(ModelDictionary, self).get(self.meta(model), default)

    def pop(self, model, *args):
        return super(ModelDictionary, self).pop(self.meta(model), *args)

    def meta(self, model):
        return getattr(model, '_meta', model)


class TransactionModel(object):
    '''A :class:`SessionModel` is the container of all objects for a given
:class:`Model` in a stdnet :class:`Session`.'''
    def __init__(self, manager):
        self.manager = manager
        self._new = OrderedDict()
        self._deleted = OrderedDict()
        self._delete_query = []
        self._modified = OrderedDict()
        self._queries = []


class Transaction(EventHandler):
    '''Transaction class for pipelining commands to :class:`.Store`.

    A :class:`Transaction` is usually obtained via the :meth:`.Mapper.begin`
    method::

        t = models.begin()

    or using the ``with`` context manager::

        with models.begin() as t:
            ...

    .. attribute:: name

        Optional :class:`Transaction` name

    .. attribute:: mapper

        the :class:`.Mapper` which initialised this transaction.

    .. attribute:: _commands

        dictionary of commands for each :class:`.Store` in this transaction.

    .. attribute:: deleted

        Dictionary of list of ids deleted from the backend server after a
        commit operation. This dictionary is only available once the
        transaction has :attr:`finished`.

    .. attribute:: saved

        Dictionary of list of ids saved in the backend server after a commit
        operation. This dictionary is only available once the transaction has
        :attr:`finished`.
    '''
    MANY_TIMES_EVENTS = ('pre_commit', 'pre_delete',
                         'post_commit', 'post_delete')

    def __init__(self, mapper, name=None):
        super(Transaction, self).__init__()
        self._loop = mapper._loop
        self.name = name or 'transaction'
        self.mapper = mapper
        self._commands = {}
        self._executed = None
        self.copy_many_times_events(mapper)

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        if type is None:
            self.commit()

    def add(self, model):
        '''Add a ``model`` to the transaction.

        :parameter model: a :class:`.Model` instance. It must be registered
            with the :attr:`mapper` which created this :class:`Transaction`.
        :return: the ``model``.
        '''
        manager = self.mapper[model]
        store = manager._store
        if store not in self._commands:
            self._commands[store] = []
        self._commands[store].append(Command(model, Command.INSERT))

    def update(self, instance_or_query, **kw):
        '''Update an ``instance`` or a ``query``'''
        if isinstance(instance_or_query, Model):
            pkvalue = instance_or_query.pkvalue()
            data = dict(instance_or_query)
            data.update(kw)
        manager = self.mapper[model]
        store = manager._store
        if store not in self._commands:
            self._commands[store] = []
        self._commands[store].append(Command(instance_or_query,
                                             Command.UPDATE))

    def execute(self, *args, **kw):
        '''Queue a command in the default data store.
        '''
        store = kw.get('store') or self.mapper._default_store
        if store not in self._commands:
            self._commands[store] = []
        self._commands[store].append(Command(args))

    def model(self, model):
        '''Returns the :class:`TransactionModel` for ``model``
        '''
        manager = self.mapper[model]
        sm = self._models.get(manager)
        if sm is None:
            sm = TransactionModel(manager)
            self._models[manager] = sm
        return sm

    def commit(self):
        if self._executed is None:
            self._executed = self._commit()
            return self._executed
        else:
            raise InvalidOperation('Transaction already executed.')

    def wait(self, callback=None):
        '''Waits for the transaction have finished.

        :param callback: optional function called back once the transaction
            has finished. The function receives one parameter only, the
            transaction.
        :return: a :class:`.Deferred`
        '''
        if self._executed is None:
            self.commit()
        if callback:
            return self._executed.then().add_callback(lambda r: callback(self))
        else:
            return self._executed

    @in_loop
    def _commit(self):
        results = []
        for store, commands in iteritems(self._commands):
            result = yield store.execute_transaction(commands)
            results.append(result)
        coroutine_return(results)
