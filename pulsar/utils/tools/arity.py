import inspect

__all__ = ['checkarity']


def checkarity(func, args, kwargs, discount = 0):
    '''Check if arguments respect a given function arity and return
a error message if the check did not pass, otherwise it returns ``None``.

:parameter func: the function.
:parameter args: function arguments.
:parameter kwargs: function key-valued parameters.
:parameter discount: optional integer which discount the number of
                     positional argument to check. Default ``0``.'''
    spec = inspect.getargspec(func)
    len_defaults = 0 if not spec.defaults else len(spec.defaults)
    len_args = len(spec.args) - discount
    len_args_input = len(args)
    minlen = len_args - len_defaults
    totlen = len_args_input + len(kwargs)
    maxlen = len_args
    if spec.varargs or spec.keywords:
        maxlen = None
    if not spec.defaults and maxlen:
        start = '"{0}" takes'.format(func.__name__)
    else:
        start = '"{0}" takes at least'.format(func.__name__)
    if totlen < minlen:
        return '{0} {1} parameters. {2} given.'.format(start,minlen,totlen)
    elif maxlen and totlen > maxlen:
        return '{0} {1} parameters. {2} given.'.format(start,minlen,totlen)
    
    # Length of parameter OK, check names
    if len_args_input < len_args:
        for arg in spec.args[discount+len_args_input:]:
            kwargs.pop(arg,None)
        if kwargs:
            s = ''
            if len(kwargs) > 1:
                s = 's'
            p = ', '.join('"{0}"'.format(p) for p in kwargs)
            return '"{0}" does not accept {1} parameter{2}.'.format(func.__name__,p,s)


                
        
        
        