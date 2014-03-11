

class SearchEngine(object):

    def set_mapper(self, mapper):
        '''Set the :class:`.Mapper` for this search engine.
        '''


def register_serachengine(name, engine_class):
    _search_engines[name] = engine_class


def search_engine(url):
    return None
