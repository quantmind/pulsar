import logging

from pulsar import ispy3k

if ispy3k:
    from xmlrpc import client as rpc
else:
    import xmlrpclib as rpc
    
    
__all__ = ['INVALID_RPC','METHOD_NOT_FOUND','INVALID_METHOD_PARAMS',
           'INTERNAL_ERROR','Fault','InternalError','InvalidRequest',
           'NoSuchFunction','InvalidParams']

_errclasses = {}
INVALID_RPC           = rpc.INVALID_XMLRPC
METHOD_NOT_FOUND      = rpc.METHOD_NOT_FOUND
INVALID_METHOD_PARAMS = rpc.INVALID_METHOD_PARAMS
INTERNAL_ERROR        = rpc.INTERNAL_ERROR


class Fault(rpc.Fault):
    
    def __init__(self, faultCode, faultString, **extra):
        rpc.Fault.__init__(self, faultCode, faultString)
        self.extra = extra
        
    def __str__(self):
        return str(self.faultString)
    
    def __repr__(self):
        return '{0} ({1}): {2}'.format(self.__class__.__name__,self.faultCode,
                                       self.faultString)
        
        
class InternalError(Fault):
    
    def __init__(self, faultString, frames = None):
        self.frames = frames or []
        super(InternalError,self).__init__(INTERNAL_ERROR, faultString)
_errclasses[INTERNAL_ERROR] = InternalError
        

class InvalidRequest(Fault):
    def __init__(self, faultString, **extra):
        super(InvalidRequest,self).__init__(INVALID_RPC, faultString, **extra)
_errclasses[INVALID_RPC] = InvalidRequest


class NoSuchFunction(InvalidRequest):
    def __init__(self, faultString, **extra):
        super(NoSuchFunction,self).__init__(faultString, **extra)
        self.faultCode = METHOD_NOT_FOUND
_errclasses[METHOD_NOT_FOUND] = NoSuchFunction 


class InvalidParams(InvalidRequest):
    def __init__(self, faultString, **extra):
        super(InvalidParams,self).__init__(faultString, **extra)
        self.faultCode = INVALID_METHOD_PARAMS
_errclasses[INVALID_METHOD_PARAMS] = InvalidParams


def handle(err):
    if isinstance(err, Fault):
        return err
    return Fault(INTERNAL_ERROR, str(err))


def exception(code, message, **extra):
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
