from datetime import date, timedelta
from random import uniform, randint, choice

from pulsar.utils.string import random_string


def def_converter(x):
    return x


def populate(datatype='string', size=10, start=None, end=None,
             converter=None, choice_from=None, **kwargs):
    '''Utility function for populating lists with random data.

    Useful when populating database with data for fuzzy testing.

    Supported data-types

    * *string*
        For example::

            populate('string', 100, min_len=3, max_len=10)

        create a 100 elements list with random strings
        with random length between 3 and 10

    * *date*
        For example::

            from datetime import date
            populate('date', 200, start=date(1997,1,1), end=date.today())

        create a 200 elements list with random datetime.date objects
        between *start* and *end*

    * *integer*
        For example::

            populate('integer', 200, start=0, end=1000)

        create a 200 elements list with random integers between ``start``
        and ``end``

    * *float*
        For example::

            populate('float', 200, start=0, end=10)

        create a 200 elements list with random floats between ``start`` and
        ``end``

    * *choice* (elements of an iterable)
        For example::

            populate('choice', 200, choice_from=['pippo','pluto','blob'])

        create a 200 elements list with random elements from ``choice_from``
    '''
    data = []
    converter = converter or def_converter
    if datatype == 'date':
        date_end = end or date.today()
        date_start = start or date(1990, 1, 1)
        delta = date_end - date_start
        for s in range(size):
            data.append(converter(random_date(date_start, delta.days)))
    elif datatype == 'integer':
        start = start or 0
        end = end or 1000000
        for s in range(size):
            data.append(converter(randint(start, end)))
    elif datatype == 'float':
        start = start or 0
        end = end or 10
        for s in range(size):
            data.append(converter(uniform(start, end)))
    elif datatype == 'choice' and choice_from:
        for s in range(size):
            data.append(choice(list(choice_from)))
    else:
        for s in range(size):
            data.append(converter(random_string(**kwargs)))
    return data


def random_date(date_start, delta):
    return date_start + timedelta(days=randint(0, delta))
