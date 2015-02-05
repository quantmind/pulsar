import sys
import traceback


def is_relevant_tb(tb):
    return '__skip_traceback__' not in tb.tb_frame.f_locals


def tb_length(tb):
    length = 0
    while tb and is_relevant_tb(tb):
        length += 1
        tb = tb.tb_next
    return length


def _tarceback_list(exctype, value, tb, trace=None):
    while tb and not is_relevant_tb(tb):
        tb = tb.tb_next
    length = tb_length(tb)
    if length or not trace:
        tb = traceback.format_exception(exctype, value, tb, length)
    if trace:
        if tb:
            tb = tb[:-1]
            tb.extend(trace[1:])
        else:
            tb = trace
    return tb


def format_traceback(exc):
    return _tarceback_list(exc.__class__, exc, exc.__traceback__)
