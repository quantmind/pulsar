#First try local
try:
    from ._pulsar import *
    hasextensions = True    
except ImportError:
    # Try Global
    try:
        from _pulsar import *
        hasextensions = True
    except ImportError:
        hasextensions = False
        from .fallback import *

HTTP_REQUEST = 0
HTTP_RESPONSE = 1
HTTP_BOTH = 2

from . import fallback


Http_Parser = HttpParser

def setDefaultHttpParser(parser):
    global Http_Parser
    Http_Parser = parser