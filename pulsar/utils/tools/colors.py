"""
Functions for wrapping strings in ANSI color codes.

Each function within this module returns the input string ``text``, wrapped
with ANSI color codes for the appropriate color.

For example, to print some text as green on supporting terminals::

    from pulsar.utils.colors import green

    print(green("This text is green!"))

Because these functions simply return modified strings, you can nest them::

    from fabric.colors import red, green

    print(red("This sentence is red, except for " + green("these words, which are green") + "."))

If ``bold`` is set to ``True``, the ANSI flag for bolding will be flipped on
for that particular invocation, which usually shows up as a bold or brighter
version of the original color on most terminals.
"""
import logging
from pulsar import to_string, platform

__all__ = ['ColorFormatter']

BLACK, RED, GREEN, YELLOW, BLUE, MAGENTA, CYAN, WHITE = range(8)

def _wrap_with(name,code):
    def inner(text, bold=False):
        c = code
        if bold:
            c = "1;{0}".format(c)
        return "\033[{0}m{1}\033[0m".format(c, text)
    
    _COLOR_MAP[name] = inner
    return inner

_COLOR_MAP = {}
red = _wrap_with('red','31')
green = _wrap_with('green','32')
yellow = _wrap_with('yellow','33')
blue = _wrap_with('blue','34')
magenta = _wrap_with('magenta','35')
cyan = _wrap_with('cyan','36')
white = _wrap_with('white','37')


class ColorFormatter(logging.Formatter):
    
    COLORS = {"DEBUG": "blue",
              "WARNING": "yellow",
              "ERROR": "red",
              "CRITICAL": "red",
              "default": "green"}
    
    def __init__(self, *args, **kwargs):
        self.use_color = kwargs.pop('use_color',not platform.isWindows())
        logging.Formatter.__init__(self, *args, **kwargs)

    def formatException(self, ei):
        r = logging.Formatter.formatException(self, ei)
        return to_string(r,"utf-8","replace")

    def format(self, record):
        levelname = record.levelname

        if self.use_color:
            COLORS = self.COLORS
            if not levelname in COLORS:
                levelname = 'default'
            if levelname in COLORS:
                name = COLORS[levelname]
                if name in _COLOR_MAP:
                    wrap = _COLOR_MAP[name]
                    record.msg = wrap(record.msg)

        return logging.Formatter.format(self, record)
