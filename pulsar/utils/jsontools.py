import time
from datetime import date, datetime
from decimal import Decimal
import json


def totimestamp(dte):
    return time.mktime(dte.timetuple())

def totimestamp2(dte):
    return totimestamp(dte) + 0.000001*dte.microsecond

def todatetime(tstamp):
    return datetime.fromtimestamp(tstamp)


class JSONDateDecimalEncoder(json.JSONEncoder):
    """
    Provide custom serializers for JSON-RPC.
    """
    def default(self, obj):
        if isinstance(obj,datetime):
            return {'__datetime__':totimestamp2(obj)}
        elif isinstance(obj, date):
            return {'__date__':totimestamp(obj)}
        elif isinstance(obj, Decimal):
            return {'__decimal__':str(obj)}
        elif hasattr(obj,'__dict__') and hasattr(obj,'__class__'):
            d = obj.__dict__.copy()
            d['__class__'] = obj.__class__.__name__
            return d
        else:
            raise ValueError("%r is not JSON serializable" % (obj,))
        
def date_decimal_hook(dct):
    if '__datetime__' in dct:
        return todatetime(dct['__datetime__'])
    elif '__date__' in dct:
        return todatetime(dct['__date__']).date()
    elif '__decimal__' in dct:
        return Decimal(dct['__decimal__'])
    else:
        return dct
    
    
DefaultJSONEncoder = JSONDateDecimalEncoder
DefaultJSONHook = date_decimal_hook

