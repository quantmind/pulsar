import logging

from pulsar.utils.py2py3 import ispy3k

if ispy3k:
    from xmlrpc import client as rpc
else:
    import xmlrpclib as rpc

_errclasses = {}
INVALID_JSONRPC       = rpc.INVALID_XMLRPC
METHOD_NOT_FOUND      = rpc.METHOD_NOT_FOUND
INVALID_METHOD_PARAMS = rpc.INVALID_METHOD_PARAMS
INTERNAL_ERROR        = rpc.INTERNAL_ERROR

# Custom errors.
METHOD_NOT_CALLABLE   = -32604


class Fault(rpc.Fault):
    
    def __init__(self, faultCode, faultString, **extra):
        rpc.Fault.__init__(self, faultCode, faultString)
        self.extra = extra
        
        
class InternalError(Fault):
    
    def __init__(self, faultString, frames = None):
        self.frames = frames or []
        super(InternalError,self).__init__(INTERNAL_ERROR, faultString)
        

class InvalidRequest(Fault):
    def __init__(self, faultString, **extra):
        super(InvalidRequest,self).__init__(INVALID_JSONRPC, faultString, **extra)
_errclasses[INVALID_JSONRPC] = InvalidRequest


class NoSuchFunction(Fault):
    def __init__(self, faultString, **extra):
        super(NoSuchFunction,self).__init__(METHOD_NOT_FOUND, faultString, **extra)
_errclasses[METHOD_NOT_FOUND] = NoSuchFunction 


class MethodNotCallable(Fault):
    def __init__(self, faultString, **extra):
        super(MethodNotCallable,self).__init__(METHOD_NOT_CALLABLE, faultString, **extra)
_errclasses[METHOD_NOT_CALLABLE] = MethodNotCallable


class InvalidParams(Fault):
    def __init__(self, faultString, **extra):
        super(InvalidParams,self).__init__(INVALID_METHOD_PARAMS, faultString, **extra)
_errclasses[INVALID_METHOD_PARAMS] = InvalidParams


def handle(err):
    if isinstance(err, Fault):
        return err
    return Fault(INTERNAL_ERROR, str(err))


def error(code, message, **extra):
    klass = _errclasses.get(code, None)
    if klass:
        return klass(message, **extra)
    else:
        return Fault(code, message, **extra)

class JSONEncodeException(Exception):
    '''JSON RPC encode error'''
    pass


class ConcurrencyError(Exception):
    pass


class TimeLimitExceeded(ConcurrencyError):
    """The time limit has been exceeded and the job has been terminated."""
    pass


class SoftTimeLimitExceeded(ConcurrencyError):
    """The soft time limit has been exceeded. This exception is raised
    to give the task a chance to clean up."""
    pass


class AlreadyStarted(ConcurrencyError):
    '''ConcurrentPool already started'''

    
class TimeoutError(ConcurrencyError):
    pass


class MaybeEncodingError(Exception):
    """Wraps unpickleable object."""

    def __init__(self, exc, value):
        self.exc = str(exc)
        self.value = repr(value)
        super(MaybeEncodingError, self).__init__(self.exc, self.value)

    def __repr__(self):
        return "<MaybeEncodingError: %s>" % str(self)

    def __str__(self):
        return "Error sending result: '%s'. Reason: '%s'." % (
                    self.value, self.exc)
    
    
class SchedulingError(Exception):
    pass
