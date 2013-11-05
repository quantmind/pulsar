from asyncstore import odm, Compiler

class RedisNode:
    def __init__(self, name, value=None):
        self.name = name
        self.value = value


class RedisQuery(odm.StoreQuery):
    card = None

    def process(self, store, pipe=None):
        self.pipe = store.pipeline() if pipe is None else pipe
        meta, keys, nodes = self._meta, [], []
        if not self.where:
            node = RedisNode('idset')
        elif len(where) == 1:
            node = RedisNode(*self.where[0])
        else:
            node = RedisNode('intercept', [RedisNode(*w) for w in self.where])

        if self.exclude:
            if len(self.exclude):
                exclude = RedisNode('union', [RedisNode(*w) for w in
                                              self.exclude])
            else:
                exclude = RedisNode(*self.exclude[0])
            node = RedisNode('difference', (node, exclude))
        if self.unions:
            node = RedisNode('union', [node] + self.unions)

        temp_key = True
        if node.name == 'idset':
            key = store.basekey(meta, 'id')
            temp_key = False
        elif node.name == 'set':
            key = node.tempkey(meta)
            keys.insert(0, key)
            pipe.execute_script('odmrun', keys, 'query', self.meta_info,
                                qs.name, *args)
        else:
            key = store.tempkey(meta)
            p = 'z' if meta.ordering else 's'
            pipe.execute_script('move2set', keys, p)
            if qs.name == 'intersect':
                command = getattr(pipe, p+'interstore')
            elif qs.name == 'union':
                command = getattr(pipe, p+'unionstore')
            elif qs.name == 'diff':
                command = getattr(pipe, p+'diffstore')
            else:
                raise ValueError('Could not perform %s operation' % qs.name)
            command(key, keys)
        if temp_key:
            pipe.expire(key, self.expire)
        self.query_key = key

    def count(self):
        '''Execute the query without fetching data. Returns the number of
        elements in the query.'''
        pipe = self.pipe
        if not self.card:
            if self._meta.ordering:
                self.card = getattr(pipe, 'zcard')
            else:
                self.card = getattr(pipe, 'scard')
        self.card(self.query_key)
        result = yield pipe.execute()
        yield result[-1]


class RedisCompiler(Compiler):

    def __init__(self, store):
        self.store = store

    def compile_query(self, query):
        '''Compile a query into '''
        return RedisQuery(self.store, query)
