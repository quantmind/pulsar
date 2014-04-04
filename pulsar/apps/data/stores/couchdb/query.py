from pulsar import coroutine_return
from pulsar.apps.http import HttpClient
from pulsar.apps.data import odm


DEFAULT_HEADERS = {'Accept': 'application/json, text/plain; q=0.8',
                   'content-type': 'application/json'}


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


class CouchDBMixin(object):

    def _init(self, headers=None, namespace=None, **kw):
        if not self._database:
            self._database = namespace or 'defaultdb'
        elif namespace:
            self._database = '%s_%s' % (self._database, namespace)
        bits = self._name.split('+')
        if len(bits) == 2:
            self._scheme = bits[0]
            self._name = bits[1]
        else:
            self._scheme = 'http'
        host = self._host
        if isinstance(host, tuple):
            host = '%s:%s' % host
        self._address = '%s://%s' % (self._scheme, host)
        self.headers = DEFAULT_HEADERS.copy()
        if headers:
            self.headers.update(headers)
        self._http = HttpClient(loop=self._loop, **kw)


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
