import string
import re
from uuid import uuid4
from random import randint, choice


_characters = string.ascii_letters + string.digits


def to_bytes(s, encoding=None, errors=None):
    '''Convert *s* into bytes'''
    if not isinstance(s, bytes):
        return ('%s' % s).encode(encoding or 'utf-8', errors or 'strict')
    elif not encoding or encoding == 'utf-8':
        return s
    else:
        d = s.decode('utf-8')
        return d.encode(encoding, errors or 'strict')


def to_string(s, encoding=None, errors='strict'):
    """Inverse of to_bytes"""
    if isinstance(s, bytes):
        return s.decode(encoding or 'utf-8', errors)
    elif not isinstance(s, str):
        return str(s)
    else:
        return s


def native_str(s, encoding=None):
    if isinstance(s, bytes):
        return s.decode(encoding or 'utf-8')
    else:
        return s


def random_string(min_len=3, max_len=20, characters=None, **kwargs):
    characters = characters or _characters
    len = randint(min_len, max_len) if max_len > min_len else min_len
    return ''.join((choice(characters) for s in range(len)))


def gen_unique_id():
    return 'i%s' % uuid4().hex


def camel_to_dash(name):
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()
