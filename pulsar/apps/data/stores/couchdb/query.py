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
        return self._query_view(group=bool(self.aggregated))

    def all(self):
        return self._query_view(include_docs=True, reduce=False)

    def delete(self):
        return self._query_view(self._delete, reduce=False)

    def _query_view(self, callback=None, reduce=True, query_type=None, **kw):
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
            result = yield view(self._meta, view_name, reduce=reduce, **kw)
        except CouchDbNoViewError:
            raise odm.QueryError('Couchdb view for %s not available for %s' %
                                 (view_name, self._meta))
        if callback:
            query = yield callback(result)
        elif reduce:
            query = sum((q['value'] for q in result['rows']))
        else:
            query = self.models((q['doc'] for q in result['rows']))
        coroutine_return(query)

    def _delete(self, result):
        docs = [{'_id': r['id'], '_rev': r['value'], '_deleted': True} for
                r in result['rows']]
        return self._store.update_documents(self._store._database, docs)
