from pulsar.apps.data import odm


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

    def all(self):
        view = self._store.query_model_view
        if self.aggregated:
            query = None
            for field, lookups in self.aggregated.items():
                if len(lookups) > 1:
                    raise NotImplementedError
                keys = []
                for lookup in lookups:
                    if lookup.type == 'value':
                        keys.append(lookup.value)
                    else:
                        raise NotImplementedError
                q = yield view(self._meta, field, keys=keys)
                if query is None:
                    query = q['rows']
                else:
                    raise NotImplementedError
        else:
            query = yield self._store.query_model_view(self._meta, 'all')
            query = query['rows']
        return self.models((q['value'] for q in query))
