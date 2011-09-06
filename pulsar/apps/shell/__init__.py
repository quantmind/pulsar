import os
import sys
import code
from time import time

import pulsar


if pulsar.ispy3k:
    def decode_line(line):
        return line
else:
    def decode_line(line):
        encoding = getattr(sys.stdin, "encoding", None)
        if encoding and not isinstance(line, unicode):
            line = line.decode(encoding)
        return line
    

class InteractiveConsole(code.InteractiveConsole):

    def pulsar_banner(self):
        cprt = 'Type "help", "copyright", "credits" or "license" for more information.'
        return "Python %s on %s\n%s\nPulsar %s\n" %\
                    (sys.version, sys.platform, cprt,pulsar.__version__)
     
    def setup(self, banner=None):
        """Closely emulate the interactive Python console.

        The optional banner argument specify the banner to print
        before the first interaction; by default it prints a banner
        similar to the one printed by the real Python interpreter,
        followed by the current class name in parentheses (so as not
        to confuse this with the real interpreter -- since it's so
        close!).

        """
        try:
            sys.ps1
        except AttributeError:
            sys.ps1 = ">>> "
        try:
            sys.ps2
        except AttributeError:
            sys.ps2 = "... "
        if banner is None:
            banner = self.pulsar_banner()
        self.write(banner)
        self._more = 0
        
    def interact(self, timeout):
        start = time()
        while time() - start < timeout:
            if self._more:
                prompt = sys.ps2
            else:
                prompt = sys.ps1
            try:
                line = decode_line(self.raw_input(prompt))
            except EOFError:
                self.write("\n")
            else:
                self._more = self.push(line)


class PulsarShell(pulsar.Application):
    cfg = {'timeout':300,
           'worker_class':'base'}
    
    def load(self):
        imported_objects = {'pulsar':pulsar,
                            'arbiter':pulsar.arbiter(),
                            'spawn':pulsar.spawn,
                            'Actor':pulsar.Actor}
        try: # Try activating rlcompleter, because it's handy.
            import readline
        except ImportError:
            pass
        else:
            import rlcompleter
            readline.set_completer(rlcompleter.Completer(imported_objects).complete)
            readline.parse_and_bind("tab:complete")
        self.console = InteractiveConsole(imported_objects)
        self.console.setup()
        return self

    def worker_task(self, worker):
        self.console.interact(0.5*worker.timeout)

