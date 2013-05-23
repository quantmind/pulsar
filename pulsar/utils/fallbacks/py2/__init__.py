# This is for python 2.x
from inspect import istraceback
def raise_error_trace(err, traceback):
    if istraceback(traceback):
        raise err.__class__, err, traceback
    else:
        raise err.__class__, err, None