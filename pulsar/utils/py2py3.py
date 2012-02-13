'''\
Simple python script which helps writing python 2.6 \
forward compatible code with python 3'''
import os
import sys
import types

ispy3k = sys.version_info[0] == 3

UTF8 = 'utf-8'

if ispy3k: # Python 3
    ispy32 = sys.version_info[1] >= 2
    string_type = str
    itervalues = lambda d : d.values()
    iteritems = lambda d : d.items()
    int_type = int
    zip = zip
    map = map
    range = range
    from io import BytesIO, StringIO
    import pickle
    i2b = lambda n : bytes((n,))
    
    from urllib import parse as urlparse
    
    class UnicodeMixin(object):
        
        def __unicode__(self):
            return '{0} object'.format(self.__class__.__name__)
        
        def __str__(self):
            return self.__unicode__()
        
        def __repr__(self):
            return '%s: %s' % (self.__class__.__name__,self)
    
    def native_str(s):
        if isinstance(s,bytes):
            return s.decode(UTF8)
        return s
    
    def to_bytestring(s, encoding=UTF8, errors='strict'):
        if isinstance(s,bytes):
            return s
        return s.encode(encoding, errors)
    
    def execfile(filename, globals=None, locals=None):
        if globals is None:
            globals = sys._getframe(1).f_globals
        if locals is None:
            locals = sys._getframe(1).f_locals
        with open(filename, "r") as fh:
            exec(fh.read()+"\n", globals, locals)
            
    def raise_error_trace(err,traceback):
        raise err.with_traceback(traceback)
            
else: # Python 2
    ispy32 = False
    string_type = unicode
    execfile = execfile
    itervalues = lambda d : d.itervalues()
    iteritems = lambda d : d.iteritems()
    int_type = (types.IntType, types.LongType)
    from itertools import izip as zip, imap as map
    from ._py2 import *
    range = xrange
    
    i2b = lambda n : chr(n)
    
    import urlparse
    from cStringIO import StringIO
    BytesIO = StringIO
    
    import cPickle as pickle
    
    def native_str(s):
        if isinstance(s,unicode):
            return s.encode(UTF8)
        return s
    
    def to_bytestring(s, encoding=UTF8, errors='strict'):
        if isinstance(s,unicode): 
            return s.encode(encoding, errors)
        else:
            return s
    
    
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



def to_string(s, encoding=UTF8, errors='strict'):
    """Inverse of to_bytestring"""
    if isinstance(s,bytes):
        return s.decode(encoding,errors)
    
    if not is_string(s):
        s = string_type(s)
        
    return s

        