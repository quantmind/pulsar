from pulsar import get_event_loop, new_event_loop
from pulsar.apps.data import Store, parse_store_url
from pulsar.utils.exceptions import ImproperlyConfigured, InvalidOperation
from pulsar.utils.importer import module_attribute


search_engines = {}


class IndexTransaction(object):
    __slots__ = ('_engine', '_items', '_executed')

    def __init__(self, engine):
        self._engine = engine
        self._items = []
        self._executed = None

    def add(self, model):
        self._items.append(model)

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        if type is None:
            self.commit()

    def commit(self):
        if self._executed is None:
            self._executed = self.engine.upload(self._items)
            return self._executed
        else:
            raise InvalidOperation('Transaction already executed.')


class SearchEngine(Store):
    '''Interface class for a full text search engine
    '''
    _mapper = None

    def set_mapper(self, mapper):
        '''Set the :class:`.Mapper` for this search engine.
        '''
        self._mapper = mapper
        mapper.bind_event('post_commit', self.index_model)
        mapper.bind_event('post_delete', self.deindex_model)

    def begin(self):
        return IndexTransaction(self)

    def upload(self, items):
        '''Upload items to the search engine'''
        raise NotImplementedError

    def index_model(self, *args, **kw):
        pass

    def deindex_model(self, *args, **kw):
        pass

    def create_table(self, manager):
        '''Invoked when a :class:`.Manager` creates the data store tables.

        By default it does nothing, some search engine implementation
        may need to do something here
        '''


def register_searchengine(name, engine_class):
    search_engines[name] = engine_class


def search_engine(url, loop=None, **kw):
    if isinstance(url, SearchEngine):
        return url
    loop = loop or get_event_loop()
    if not loop:
        raise ImproperlyConfigured('no event loop')
    if isinstance(url, dict):
        extra = url.copy()
        url = extra.pop('url', None)
        extra.update(kw)
        kw = extra
    scheme, address, params = parse_store_url(url)
    dotted_path = search_engines.get(scheme)
    if not dotted_path:
        raise ImproperlyConfigured('%s search engine not available' % scheme)
    engine_class = module_attribute(dotted_path)
    params.update(kw)
    return engine_class(scheme, address, loop, **params)
