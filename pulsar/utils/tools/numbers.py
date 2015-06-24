from time import mktime
from datetime import datetime


__all__ = ['date2timestamp']


def date2timestamp(dte):
    '''Convert a *dte* into a valid unix timestamp.'''
    seconds = mktime(dte.timetuple())
    if isinstance(dte, datetime):
        return seconds + dte.microsecond / 1000000.0
    else:
        return int(seconds)
