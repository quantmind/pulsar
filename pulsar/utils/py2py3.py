import types

__all__ = ['string_type',
           'int_type',
           'to_bytestring',
           'to_string']

try:
    string_type = unicode    
except NameError:
    string_type = str

    
try:
    int_type = (types.IntType, types.LongType)
except AttributeError:
    int_type = int
    
    
    
def to_bytestring(s, encoding='utf-8', errors='strict'):
    """Returns a bytestring version of 's',
encoded as specified in 'encoding'.
If strings_only is True, don't convert (some)
non-string-like objects."""
    if isinstance(s,bytes):
        if encoding != 'utf-8':
            return s.decode('utf-8', errors).encode(encoding, errors)
        else:
            return s
        
    if isinstance(s, string_type):    
        return s.encode(encoding, errors)
    else:
        return s


def to_string(s, encoding='utf-8', errors='strict'):
    """Inverse of to_bytestring"""
    if isinstance(s, string_type):
        return s
    elif isinstance(s,bytes):
        return s.decode(encoding,errors)
    else:
        string_type(s)
        