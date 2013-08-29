'''\
An asynchronous shell for experimenting with pulsar on the command line.
To use write a little script, lets call it ``pshell.py``::

    from pulsar.apps.shell import PulsarShell 
    
    if __name__ == '__main__':
        PulsarShell().start()

And run it::

    python pshell.py
    
.. module:: pulsar

The shell has already ``pulsar``, :func:`get_actor`, :func:`spawn`,
:func:`send` and the :class:`Actor` class in the global dictionary::

    >>> pulsar.__version__
    0.5.0
    >>> actor = get_actor()
    >>> actor.info_state
    'running'
    >>> a = spawn()
    >>> a.done()
    True
    >>> proxy = a.result


Implementation
~~~~~~~~~~~~~~~~~~~~

.. autoclass:: pulsar.apps.shell.PulsarShell
   :members:
   :member-order: bysource
'''
import sys
import code
from time import time

import pulsar
from pulsar.utils.pep import ispy3k


if ispy3k:
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
        """Closely emulate the interactive Python console."""
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
    name = 'shell'
    cfg = pulsar.Config(loglevel='none', process_name='Pulsar shell',
                        console_class=InteractiveConsole)
    
    def monitor_start(self, monitor):
        monitor.cfg.set('workers', 1)
        monitor.cfg.set('concurrency', 'thread')
        
    def worker_start(self, worker):  #pragma    nocover
        '''When the worker starts, create the :attr:`Actor.thread_pool`
with one thread only and send the :meth:`interact` method to it.'''
        worker.create_thread_pool()
        worker.thread_pool.apply(self.start_shell, worker)
        
    def start_shell(self, worker):
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
        self.local.console = self.cfg.console_class(imported_objects)
        self.local.console.setup()
        worker.thread_pool.apply(self.interact, worker)
                
    def interact(self, worker):
        '''Handled by the :attr:`Actor.thread_pool`'''
        try:
            self.local.console.interact(self.cfg.timeout)
            worker.thread_pool.apply(self.interact, worker)
        except:
            worker.send('arbiter', 'stop')