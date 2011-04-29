from uuid import uuid4
from hashlib import sha1

__all__ = ['gen_unique_id']

def gen_unique_id():
    return str(uuid4())


def kwargsgen(args,kwargs):
    for a in args:
        yield str(a)
    for k in sorted(kwargs):
        v = kwargs[k]
        yield '{0}={1}'.format(k,v)
            
            
def unique_function_call(name,args,kwargs):
    vname = ','.join(kwargsgen(args,kwargs))
    fname = '{0}({1})'.format(name,vname)
    return sha1(fname).hexdigest()