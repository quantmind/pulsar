#First try local
try:
    from http_parser.parser import HttpParser
    hasextensions = True
except ImportError: #pragma    nocover
    hasextensions = False
    from pulsar.utils.httpurl import HttpParser

Http_Parser = HttpParser

def setDefaultHttpParser(parser):
    global Http_Parser
    Http_Parser = parser