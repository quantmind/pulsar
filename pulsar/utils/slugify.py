'''A slugify function which handle unicode

.. autofunction:: slugify

'''
import re
from unicodedata import normalize
from html.entities import name2codepoint

try:
    from unidecode import unidecode
except ImportError:
    unidecode = None

from .pep import to_string


# character entity reference
CHAR_ENTITY_REXP = re.compile('&(%s);' % '|'.join(name2codepoint))

# decimal character reference
DECIMAL_REXP = re.compile('&#(\d+);')

# hexadecimal character reference
HEX_REXP = re.compile('&#x([\da-fA-F]+);')

REPLACE1_REXP = re.compile(r'[\']+')
REPLACE2_REXP = re.compile(r'[^-a-z0-9]+')
REMOVE_REXP = re.compile('-{2,}')


def slugify(value, separator='-', max_length=0, word_boundary=False,
            entities=True, decimal=True, hexadecimal=True):
    '''Normalizes string, removes non-alpha characters,
    and converts spaces to ``separator`` character
    '''
    value = normalize('NFKD', to_string(value, 'utf-8', 'ignore'))
    if unidecode:
        value = unidecode(value)

    # character entity reference
    if entities:
        value = CHAR_ENTITY_REXP.sub(
            lambda m: chr(name2codepoint[m.group(1)]), value)

    # decimal character reference
    if decimal:
        try:
            value = DECIMAL_REXP.sub(lambda m: chr(int(m.group(1))), value)
        except:
            pass

    # hexadecimal character reference
    if hexadecimal:
        try:
            value = HEX_REXP.sub(lambda m: chr(int(m.group(1), 16)), value)
        except:
            pass

    value = value.lower()

    value = REPLACE1_REXP.sub('', value)
    value = REPLACE2_REXP.sub('-', value)

    # remove redundant -
    value = REMOVE_REXP.sub('-', value).strip('-')

    # smart truncate if requested
    if max_length > 0:
        value = smart_truncate(value, max_length, word_boundary, '-')

    if separator != '-':
        value = value.replace('-', separator)

    return value


def smart_truncate(value, max_length=0, word_boundaries=False, separator=' '):
    """ Truncate a string """

    value = value.strip(separator)

    if not max_length:
        return value

    if len(value) < max_length:
        return value

    if not word_boundaries:
        return value[:max_length].strip(separator)

    if separator not in value:
        return value[:max_length]

    truncated = ''
    for word in value.split(separator):
        if word:
            next_len = len(truncated) + len(word) + len(separator)
            if next_len <= max_length:
                truncated += '{0}{1}'.format(word, separator)
    if not truncated:
        truncated = value[:max_length]
    return truncated.strip(separator)
