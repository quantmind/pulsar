'''\
An asynchronous shell for experimenting with pulsar on the command line.
The shell is available for posix operative systems only. Windows support
to come at some point.

To use write a little script, lets call it ``pshell.py``::

    from pulsar.apps.shell import PulsarShell 
    
    if __name__ == '__main__':
        PulsarShell().start()

And run it::

    python pshell.py
    
The shell has already ``pulsar``, ``arbiter`` and the :class:`Actor`
class in the global dictionary::

    >> pulsar.__version__
    0.2.0
    >> arbiter.is_arbiter()
    True
    >> arbiter.state
    'running'
    >> arbiter.MANAGED_ACTORS
    {}
    >> a = arbiter.spawn(Actor)
    >> a.is_alive()
    True
    >> arbiter.MANAGED_ACTORS
    {'3a67a186': 3a67a186}
    >> arbiter.close_actors()
    >> a.is_alive()
    False
    
'''
import os
import sys
import code
from time import time

import pulsar
from pulsar.apps import tasks


if pulsar.ispy3k:
    def decode_line(line):
        return line
else:   #pragma    nocover
    def decode_line(line):
        encoding = getattr(sys.stdin, "encoding", None)
        if encoding and not isinstance(line, unicode):
            line = line.decode(encoding)
        return line
    

class InteractiveConsole(code.InteractiveConsole):  #pragma    nocover

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


class PulsarShell(tasks.CPUboundServer):
    can_kill_arbiter = True
    _app_name = 'pulsarshell'
    console_class = InteractiveConsole
    cfg_apps = ('cpubound',)
    cfg = {'timeout':5,
           'workers':1,
           'loglevel':'none',
           'concurrency':'thread'}
    
    @property
    def console(self):
        return self.local.console
    
    def handler(self):
        imported_objects = {'pshell': self,
                            'pulsar': pulsar,
                            'get_actor': pulsar.get_actor,
                            'spawn': pulsar.spawn,
                            'send': pulsar.send,
                            'Actor': pulsar.Actor}
        try: # Try activating rlcompleter, because it's handy.
            import readline
        except ImportError: #pragma    nocover
            pass
        else: #pragma    nocover
            import rlcompleter
            readline.set_completer(rlcompleter.Completer(imported_objects).complete)
            readline.parse_and_bind("tab:complete")
        self.local.console = self.console_class(imported_objects)
        self.console.setup()
        return self

    def monitor_start(self, monitor):
        monitor.cfg.set('workers', 1)
        monitor.cfg.set('concurrency', 'thread')
        
    def worker_task(self, worker):  #pragma    nocover
        try:
            self.console.interact(0.5*self.cfg.timeout)
        except pulsar.EXIT_EXCEPTIONS:
            worker.send('arbiter', 'stop')
            worker.state = pulsar.ACTOR_STATES.INACTIVE
            

