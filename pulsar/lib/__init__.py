#First try local
try:
    from ._pulsar import *
    hasextensions = True
except ImportError: #pragma    nocover
    # Try Global
    try:
        from _pulsar import *
        hasextensions = True
    except ImportError:
        hasextensions = False
        from pulsar.utils.httpurl import HttpParser

Http_Parser = HttpParser

def setDefaultHttpParser(parser):
    global Http_Parser
    Http_Parser = parser