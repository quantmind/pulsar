import logging

from pulsar import ispy3k

if ispy3k:
    from xmlrpc import client as rpc
else:
    import xmlrpclib as rpc


__all__ = ['INVALID_RPC',
           'METHOD_NOT_FOUND',
           'INVALID_METHOD_PARAMS',
           'INTERNAL_ERROR',
           'REQUIRES_AUTHENTICATION',
           'Fault',
           'InternalError',
           'InvalidRequest',
           'NoSuchFunction',
           'InvalidParams',
           'RequiresAuthentication']

_errclasses = {}
INVALID_RPC = rpc.INVALID_XMLRPC
METHOD_NOT_FOUND = rpc.METHOD_NOT_FOUND
INVALID_METHOD_PARAMS = rpc.INVALID_METHOD_PARAMS
INTERNAL_ERROR = rpc.INTERNAL_ERROR
REQUIRES_AUTHENTICATION =-32000


class FaultType(type):
    '''A metaclass for rpc handlers.
Add a limited ammount of magic to RPC handlers.'''
    def __new__(cls, name, bases, attrs):
        newcls = super(FaultType, cls).__new__(cls, name, bases, attrs)
        code = attrs.get('FAULT_CODE')
        if code:
            _errclasses[code] = newcls
        return newcls


class Fault(FaultType('FaultBase', (rpc.Fault,), {})):
    FAULT_CODE = None
    def __init__(self, *args, **kwargs):
        rpc.Fault.__init__(self, self.FAULT_CODE, *args, **kwargs)

    def __repr__(self):
        return '%s %s: %s' % (self.__class__.__name__, self.faultCode,
                               self.faultString)


class InternalError(Fault):
    FAULT_CODE = INTERNAL_ERROR


class InvalidRequest(Fault):
    FAULT_CODE = INVALID_RPC


class NoSuchFunction(InvalidRequest):
    FAULT_CODE = METHOD_NOT_FOUND


class InvalidParams(InvalidRequest):
    FAULT_CODE = INVALID_METHOD_PARAMS


class RequiresAuthentication(Fault):
    FAULT_CODE = REQUIRES_AUTHENTICATION


def handle(err):
    if isinstance(err, Fault):
        return err
    return InternalError(str(err))


def exception(code, *args, **extra):
    klass = _errclasses.get(code, InternalError)
    return klass(*args, **extra)


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
