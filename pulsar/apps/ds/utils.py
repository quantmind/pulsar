import shutil
import pickle


def save_data(cfg, filename, data):
    logger = cfg.configured_logger('pulsar.ds')
    temp = 'temp_%s' % filename
    with open(temp, 'wb') as file:
        pickle.dump(data, file, protocol=2)
    shutil.move(temp, filename)
    logger.info('wrote data into "%s"', filename)


def sort_command(store, client, request, value):
    sort_type = type(value)
    right = 0
    desc = False
    alpha = None
    start = None
    end = None
    storekey = None
    sortby = None
    dontsort = False
    getops = []
    N = len(request)
    j = 2
    while j < N:
        val = request[j].lower()
        right = N - j - 1
        if val == b'asc':
            desc = False
        elif val == b'desc':
            desc = True
        elif val == b'alpha':
            alpha = True
        elif val == b'limit' and right >= 2:
            try:
                start = max(0, int(request[j+1]))
                count = int(request[j+2])
            except Exception:
                return client.error_reply(store.SYNTAX_ERROR)
            end = len(value) if count <= 0 else start + count
            j += 2
        elif val == b'store' and right >= 1:
            storekey = request[j+1]
            j += 1
        elif val == b'by' and right >= 1:
            sortby = request[j+1]
            if b'*' not in sortby:
                dontsort = True
            j += 1
        elif val == b'get' and right >= 1:
            getops.append(request[j+1])
            j += 1
        else:
            return client.error_reply(store.SYNTAX_ERROR)
        j += 1

    db = client.db
    if sort_type is store.zset_type and dontsort:
        dontsort = False
        alpha = True
        sortby = None

    vector = []
    sortable = SortableDesc if desc else Sortable
    #
    if not dontsort:
        for val in value:
            if sortby:
                byval = lookup(store, db, sortby, val)
                if byval is None:
                    vector.append((val, null))
                    continue
            else:
                byval = val
            if not alpha:
                try:
                    byval = sortable(float(byval))
                except Exception:
                    byval = null
            else:
                byval = sortable(byval)
            vector.append((val, byval))

        vector = sorted(vector, key=lambda x: x[1])
        if start is not None:
            vector = vector[start:end]
        vector = [val for val, _ in vector]
    else:
        vector = list(value)
        if start is not None:
            vector = vector[start:end]

    if storekey is None:
        if getops:
            result = []
            for val in vector:
                for getv in getops:
                    gval = lookup(store, db, getv, val)
                    result.append(gval)
            vector = result
        client.reply_multi_bulk(vector)
    else:
        if getops:
            vals = store.list_type()
            empty = b''
            for val in vector:
                for getv in getops:
                    vals.append(lookup(store, db, getv, val) or empty)
        else:
            vals = store.list_type(vector)
        if db.pop(storekey) is not None:
            store._signal(store.NOTIFY_GENERIC, db, 'del', storekey)
        result = len(vals)
        if result:
            db._data[storekey] = vals
            store._signal(store.NOTIFY_LIST, db, 'sort', storekey, result)
        client.reply_int(result)


def lookup(store, db, pattern, repl):
    if pattern == b'#':
        return repl
    key = pattern.replace(b'*', repl)
    bits = key.split(b'->', 1)
    if len(bits) == 1:
        string = db.get(key)
        return bytes(string) if isinstance(string, bytearray) else None
    else:
        key, field = bits
        hash = db.get(key)
        return hash.get(field) if isinstance(hash, store.hash_type) else None


class Null:
    __slots__ = ()

    def __lt__(self, other):
        return False

null = Null()


class Sortable:
    __slots__ = ('value',)

    def __init__(self, value):
        self.value = value

    def __lt__(self, other):
        if other is null:
            return True
        else:
            return self.value < other.value


class SortableDesc:
    __slots__ = ('value',)

    def __init__(self, value):
        self.value = value

    def __lt__(self, other):
        if other is null:
            return True
        else:
            return self.value > other.value


def count_bytes(array):
    '''Count the number of bits in a byte ``array``.

    It uses the Hamming weight popcount algorithm
    '''
    # this algorithm can be rewritten as
    # for i in array:
    #     count += sum(b=='1' for b in bin(i)[2:])
    # but this version is almost 2 times faster
    count = 0
    for i in array:
        i = i - ((i >> 1) & 0x55555555)
        i = (i & 0x33333333) + ((i >> 2) & 0x33333333)
        count += (((i + (i >> 4)) & 0x0F0F0F0F) * 0x01010101) >> 24
    return count


def and_op(x, y):
    return x & y


def or_op(x, y):
    return x | y


def xor_op(x, y):
    return x ^ y
