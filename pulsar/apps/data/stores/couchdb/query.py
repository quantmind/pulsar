from pulsar import coroutine_return
from pulsar.apps.data import odm


class CouchDbError(Exception):

    def __init__(self, error, reason):
        self.error = error
        super(CouchDbError, self).__init__(reason)


class CouchDbNoDbError(CouchDbError):
    pass


class CouchDbNoViewError(CouchDbError):
    pass


error_classes = {'no_db_file': CouchDbNoDbError,
                 'missing_named_view': CouchDbNoViewError}


def couch_db_error(error=None, reason=None, **params):
    error_class = error_classes.get(reason, CouchDbError)
    raise error_class(error, reason)


class CauchDbQuery(odm.CompiledQuery):
    '''Implements the CompiledQuery for couchdb
    '''
    def _build(self):
        self.aggregated = None
        query = self._query
        if query._excludes:
            raise NotImplementedError
        if query._filters:
            self.aggregated = self.aggregate(query._filters)

    def count(self):
        return self._query_view('count', group=bool(self.aggregated))

    def all(self):
        return self._query_view()

    def _query_view(self, query_type=None, **kw):
        view = self._store.query_model_view
        keys = None
        if self.aggregated:
            view_name = None
            for field, lookups in self.aggregated.items():
                keys = []
                for lookup in lookups:
                    if lookup.type == 'value':
                        keys.append(lookup.value)
                    else:
                        raise NotImplementedError
                if view_name:
                    raise NotImplementedError
                else:
                    view_name = field
        else:
            view_name = 'id'

        if query_type:
            view_name = '%s_%s' % (view_name, query_type)
        kw['keys'] = keys
        try:
            result = yield view(self._meta, view_name, **kw)
        except CouchDbNoViewError:
            raise odm.QueryError('Couchdb view for %s not available' %
                                 view_name)
        query = result['rows']
        if query_type == 'count':
            query = sum((q['value'] for q in query))
        elif not query_type:
            query = self.models((q['value'] for q in result['rows']))
        coroutine_return(query)
