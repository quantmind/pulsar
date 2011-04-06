'''\
Simple python script which helps writing python 2.6 \
forward compatible code with python 3'''
import os
import sys
import types

ispy3k = int(sys.version[0]) >= 3

UTF8 = 'utf-8'

if ispy3k: # Python 3
    string_type = str
    itervalues = lambda d : d.values()
    iteritems = lambda d : d.items()
    int_type = int
    zip = zip
    map = map
    range = range
    
    from urllib import parse as urlparse
    from io import StringIO
    
    class UnicodeMixin(object):
        
        def __unicode__(self):
            return '{0} object'.format(self.__class__.__name__)
        
        def __str__(self):
            return self.__unicode__()
        
        def __repr__(self):
            return '%s: %s' % (self.__class__.__name__,self)
        
else: # Python 2
    string_type = unicode
    itervalues = lambda d : d.itervalues()
    iteritems = lambda d : d.iteritems()
    int_type = (types.IntType, types.LongType)
    from itertools import izip as zip, imap as map
    range = xrange
    
    import urlparse
    from cStringIO import StringIO
    
    class UnicodeMixin(object):
        
        def __unicode__(self):
            return unicode('{0} object'.format(self.__class__.__name__))
        
        def __str__(self):
            return self.__unicode__().encode()
        
        def __repr__(self):
            return '%s: %s' % (self.__class__.__name__,self)
    

is_int = lambda x : isinstance(x,int_type)
is_string = lambda x : isinstance(x,string_type)
is_bytes_or_string = lambda x : isinstance(x,string_type) or isinstance(x,bytes)



def to_bytestring(s, encoding=UTF8, errors='strict'):
    """Returns a bytestring version of 's',
encoded as specified in 'encoding'."""
    if isinstance(s,bytes):
        if encoding != UTF8:
            return s.decode(UTF8, errors).encode(encoding, errors)
        else:
            return s
        
    if not is_string(s):
        s = string_type(s)    
    
    return s.encode(encoding, errors)


def to_string(s, encoding=UTF8, errors='strict'):
    """Inverse of to_bytestring"""
    if isinstance(s,bytes):
        return s.decode(encoding,errors)
    
    if not is_string(s):
        s = string_type(s)
        
    return s

        