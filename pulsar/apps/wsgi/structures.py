import re
import codecs

from pulsar.utils.structures import FrozenDict


_locale_delim_re = re.compile(r'[_-]')


def order(values):
    same = {}
    for v, q in values:
        if q not in same:
            same[q] = []
        same[q].append(v)
    for q in reversed(sorted(same)):
        for v in same[q]:
            yield v, q


class Accept(tuple):
    """An :class:`Accept` object is a tuple subclass for tuples of
    ``(value, quality)`` tuples.  It is automatically sorted by quality.

    All :class:`Accept` objects work similar to a list but provide extra
    functionality for working with the data.  Containment checks are
    normalised to the rules of that header:

    >>> a = CharsetAccept([('ISO-8859-1', 1), ('utf-8', 0.7)])
    >>> a.best
    'ISO-8859-1'
    >>> 'iso-8859-1' in a
    True
    >>> 'UTF8' in a
    True
    >>> 'utf7' in a
    False

    To get the quality for an item you can use normal item lookup:

    >>> print(a['utf-8'])
    0.7
    >>> a(['utf7'])
    0
    """
    def __new__(cls, values=None):
        values = order(values) if values else ()
        return super().__new__(cls, values)

    def _value_matches(self, value, item):
        """Check if a value matches a given accept item."""
        return item == '*' or item.lower() == value.lower()

    def quality(self, key):
        """Returns the quality of the key.

        .. versionadded:: 0.6
           In previous versions you had to use the item-lookup syntax
           (eg: ``obj[key]`` instead of ``obj.quality(key)``)
        """
        for item, quality in self:
            if self._value_matches(key, item):
                return quality
        return 0

    def __contains__(self, value):
        for item, quality in self:
            if self._value_matches(value, item):
                return True
        return False

    def __repr__(self):
        return '%s([%s])' % (
            self.__class__.__name__,
            ', '.join('(%r, %s)' % (x, y) for x, y in self)
        )

    def index(self, key):
        """Get the position of an entry or raise :exc:`ValueError`.

        :param key: The key to be looked up.

        .. versionchanged:: 0.5
           This used to raise :exc:`IndexError`, which was inconsistent
           with the list API.
        """
        if isinstance(key, str):
            for idx, (item, quality) in enumerate(self):
                if self._value_matches(key, item):
                    return idx
            raise ValueError(key)
        return list.index(self, key)

    def find(self, key):
        """Get the position of an entry or return -1.

        :param key: The key to be looked up.
        """
        try:
            return self.index(key)
        except ValueError:
            return -1

    def values(self):
        """Iterate over all values."""
        for item in self:
            yield item[0]

    def to_header(self):
        """Convert the header set into an HTTP header string."""
        result = []
        for value, quality in self:
            if quality != 1:
                value = '%s;q=%s' % (value, quality)
            result.append(value)
        return ','.join(result)

    def __str__(self):
        return self.to_header()

    def best_match(self, matches, default=None):
        """Returns the best match from a list of possible matches based
        on the quality of the client.  If two items have the same quality,
        the one is returned that comes first.

        :param matches: a list of matches to check for
        :param default: the value that is returned if none match
        """
        if matches:
            best_quality = -1
            result = default
            for client_item, quality in self:
                for server_item in matches:
                    if quality <= best_quality:
                        break
                    if self._value_matches(server_item, client_item):
                        best_quality = quality
                        result = server_item
            return result
        else:
            return self.best

    @property
    def best(self):
        """The best match as value."""
        if self:
            return self[0][0]


class ContentAccept(Accept):
    """Like :class:`Accept` but with special methods and behaviour for
    content types.
    """

    def _value_matches(self, value, item):
        def _normalize(x):
            x = x.lower()
            return x == '*' and ('*', '*') or x.split('/', 1)

        # this is from the application which is trusted.  to avoid developer
        # frustration we actually check these for valid values
        if '/' not in value:
            raise ValueError('invalid mimetype %r' % value)
        value_type, value_subtype = _normalize(value)
        if value_type == '*' and value_subtype != '*':
            raise ValueError('invalid mimetype %r' % value)

        if '/' not in item:
            return False
        item_type, item_subtype = _normalize(item)
        if item_type == '*' and item_subtype != '*':
            return False
        return (
            (item_type == item_subtype == '*' or
             value_type == value_subtype == '*') or
            (item_type == value_type and (item_subtype == '*' or
                                          value_subtype == '*' or
                                          item_subtype == value_subtype))
        )

    @property
    def accept_html(self):
        """True if this object accepts HTML."""
        return (
            'text/html' in self or
            'application/xhtml+xml' in self or
            self.accept_xhtml
        )

    @property
    def accept_xhtml(self):
        """True if this object accepts XHTML."""
        return (
            'application/xhtml+xml' in self or
            'application/xml' in self
        )

    @property
    def accept_json(self):
        """True if this object accepts JSON."""
        return 'application/json' in self


class LanguageAccept(Accept):
    """Like :class:`Accept` but with normalisation for languages."""

    def _value_matches(self, value, item):
        def _normalize(language):
            return _locale_delim_re.split(language.lower())
        return item == '*' or _normalize(value) == _normalize(item)


class CharsetAccept(Accept):
    """Like :class:`Accept` but with normalisation for charsets."""

    def _value_matches(self, value, item):
        def _normalize(name):
            try:
                return codecs.lookup(name).name
            except LookupError:
                return name.lower()
        return item == '*' or _normalize(value) == _normalize(item)


class RequestCacheControl(FrozenDict):
    pass
