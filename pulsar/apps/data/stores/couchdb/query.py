from pulsar.apps.data import odm


class CauchDbQuery(odm.CompiledQuery):
    '''Implements the CompiledQuery for couchdb
    '''
    def _build(self):
        self.aggregated = []
        query = self._query
        if query._excludes:
            raise NotImplementedError
        if query._filters:
            self.aggregated.append(self.aggregate(query._filters))

    def all(self):
        if self.aggregated:
            raise NotImplementedError
        else:
            query = yield self._store.query_model_view(self._meta, 'all')
        return self.models((q['value'] for q in query['rows']))
