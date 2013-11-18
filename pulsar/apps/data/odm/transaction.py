from pulsar import EventHandler, coroutine_return, in_loop
from pulsar.utils.pep import iteritems


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


class Command(object):
    __slots__ = ('args', 'action')

    def __init__(self, args, action=0):
        self.args = args
        self.action = action

INSERT = 1
DELETE = 2


class Transaction(EventHandler):
    '''Transaction class for pipelining commands to :class:`.Store`.

    A :class:`Transaction` is usually obtained via the :meth:`.Store.begin`
    method::

        t = store.begin()

    or using the ``with`` context manager::

        with store.begin() as t:
            ...

    **ATTRIBUTES**

    .. attribute:: session

        :class:`Session` which is being transacted.

    .. attribute:: name

        Optional :class:`Transaction` name

    .. attribute:: backend

        the :class:`stdnet.BackendDataServer` to which the transaction
        is being performed.

    .. attribute:: signal_commit

        If ``True``, a signal for each model in the transaction is triggered
        just after changes are committed.
        The signal carries a list of updated ``instances`` of the model,
        the :class:`Session` and the :class:`Transaction` itself.

        default ``True``.

    .. attribute:: signal_delete

        If ``True`` a signal for each model in the transaction is triggered
        just after deletion of instances.
        The signal carries the list ``ids`` of deleted instances of the mdoel,
        the :class:`Session` and the :class:`Transaction` itself.

        default ``True``.

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

    def __init__(self, router, name=None):
        super(Transaction, self).__init__()
        self._loop = router._loop
        self.name = name or 'transaction'
        self.router = router
        self._commands = {}
        self._executed = None
        self.copy_many_times_events(router)

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        if type is None:
            self.commit()

    def add(self, model):
        '''Add a ``model`` to the transaction.

        :parameter model: a :class:`Model` instance. It must be registered
            with the :attr:`router` which created this :class:`Transaction`.
        :return: the ``model``.
        '''
        manager = self.router[model]
        store = manager._store
        if store not in self._commands:
            self._commands[store] = []
        self._commands[store].append(Command(model, INSERT))

    def execute(self, *args, **kw):
        '''Queue a command in the default data store.
        '''
        store = kw.get('store') or self.router._default_store
        if store not in self._commands:
            self._commands[store] = []
        self._commands[store].append(Command(args))

    def model(self, model):
        '''Returns the :class:`SessionModel` for ``model`` which
can be :class:`Model`, or a :class:`MetaClass`, or an instance
of :class:`Model`.'''
        manager = self.router[model]
        sm = self._models.get(manager)
        if sm is None:
            sm = TransactionModel(manager)
            self._models[manager] = sm
        return sm

    def commit(self):
        if self._executed is None:
            self._executed = self._commit()
            return self._executed

    def wait(self):
        if self._executed is None:
            raise RuntimeError('to wait you need to commit first')
        return self._executed

    @in_loop
    def _commit(self):
        results = []
        for store, commands in iteritems(self._commands):
            result = yield store.execute_transaction(commands)
            results.append(result)
        coroutine_return(results)
