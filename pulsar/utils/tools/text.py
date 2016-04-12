from itertools import zip_longest

__all__ = ['grouper', 'num2eng', 'nice_number']


# Tokens from 1000 and up
_PRONOUNCE = ['', 'thousand', 'million', 'billion', 'trillion']

# Tokens up to 90
_SMALL = {
    '1': 'one',
    '2': 'two',
    '3': 'three',
    '4': 'four',
    '5': 'five',
    '6': 'six',
    '7': 'seven',
    '8': 'eight',
    '9': 'nine',
    '10': 'ten',
    '11': 'eleven',
    '12': 'twelve',
    '13': 'thirteen',
    '14': 'fourteen',
    '15': 'fifteen',
    '16': 'sixteen',
    '17': 'seventeen',
    '18': 'eighteen',
    '19': 'nineteen',
    '20': 'twenty',
    '30': 'thirty',
    '40': 'forty',
    '50': 'fifty',
    '60': 'sixty',
    '70': 'seventy',
    '80': 'eighty',
    '90': 'ninety'
}


def grouper(n, iterable, padvalue=None):
    '''grouper(3, 'abcdefg', 'x') -->
    ('a','b','c'), ('d','e','f'), ('g','x','x')
    '''
    return zip_longest(*[iter(iterable)]*n, fillvalue=padvalue)


def num2eng(num):
    '''English representation of a number up to a trillion.
    '''
    num = str(int(num))  # Convert to string, throw if bad number
    if (len(num) / 3 >= len(_PRONOUNCE)):  # Sanity check
        return num
    elif num == '0':  # Zero is a special case
        return 'zero'
    pron = []  # Result accumulator
    first = True
    for pr, bits in zip(_PRONOUNCE, grouper(3, reversed(num), '')):
        num = ''.join(reversed(bits))
        n = int(num)
        bits = []
        if n > 99:  # Got hundred
            bits.append('%s hundred' % _SMALL[num[0]])
            num = num[1:]
            n = int(num)
            num = str(n)
        if (n > 20) and (n != (n // 10 * 10)):
            bits.append('%s %s' % (_SMALL[num[0] + '0'], _SMALL[num[1]]))
        elif n:
            bits.append(_SMALL[num])
        if len(bits) == 2 and first:
            first = False
            p = ' and '.join(bits)
        else:
            p = ' '.join(bits)
        if p and pr:
            p = '%s %s' % (p, pr)
        pron.append(p)
    return ', '.join(reversed(pron))


def nice_number(number, name=None, plural=None):
    if not name:
        return num2eng(number)
    elif number == 1:
        return 'one %s' % name
    else:
        if not plural:
            plural = '%ss' % name
        return '%s %s' % (num2eng(number), plural)
