import inspect

__all__ = ['checkarity']


def checkarity(func, args, kwargs, discount=0):
    '''Check if arguments respect a given function arity and return
    an error message if the check did not pass,
    otherwise it returns ``None``.

    :parameter func: the function.
    :parameter args: function arguments.
    :parameter kwargs: function key-valued parameters.
    :parameter discount: optional integer which discount the number of
                         positional argument to check. Default ``0``.
    '''
    spec = inspect.getargspec(func)
    self = getattr(func, '__self__', None)
    if self and spec.args:
        discount += 1
    args = list(args)
    defaults = list(spec.defaults or ())
    len_defaults = len(defaults)
    len_args = len(spec.args) - discount
    len_args_input = len(args)
    minlen = len_args - len_defaults
    totlen = len_args_input + len(kwargs)
    maxlen = len_args
    if spec.varargs or spec.keywords:
        maxlen = None
        if not minlen:
            return

    if not spec.defaults and maxlen:
        start = '"{0}" takes'.format(func.__name__)
    else:
        if maxlen and totlen > maxlen:
            start = '"{0}" takes at most'.format(func.__name__)
        else:
            start = '"{0}" takes at least'.format(func.__name__)

    if totlen < minlen:
        return '{0} {1} parameters. {2} given.'.format(start, minlen, totlen)
    elif maxlen and totlen > maxlen:
        return '{0} {1} parameters. {2} given.'.format(start, maxlen, totlen)

    # Length of parameter OK, check names
    if len_args_input < len_args:
        l = minlen - len_args_input
        for arg in spec.args[discount:]:
            if args:
                args.pop(0)
            else:
                if l > 0:
                    if defaults:
                        defaults.pop(0)
                    elif arg not in kwargs:
                        return ('"{0}" has missing "{1}" parameter.'
                                .format(func.__name__, arg))
                kwargs.pop(arg, None)
            l -= 1
        if kwargs and maxlen:
            s = ''
            if len(kwargs) > 1:
                s = 's'
            p = ', '.join('"{0}"'.format(p) for p in kwargs)
            return ('"{0}" does not accept {1} parameter{2}.'
                    .format(func.__name__, p, s))
    elif len_args_input > len_args + len_defaults:
        n = len_args + len_defaults
        start = '"{0}" takes'.format(func.__name__)
        return ('{0} {1} positional parameters. {2} given.'
                .format(start, n, len_args_input))
