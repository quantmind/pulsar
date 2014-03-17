from pulsar.apps.data import odm


class CauchDbQuery(odm.CompiledQuery):
    '''Implements the CompiledQuery for couchdb
    '''
    def _build(self):
        query = self.query
        if query._excludes:
            raise NotImplementedError
        if query._filters:
            aggregated = self.aggregate(query._filters)
        else:
            aggregated = 0
        for aggregate in aggregated:
            pass
