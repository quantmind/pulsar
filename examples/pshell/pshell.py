'''\
Pulsar Python Shell example::

    python pshell.py
'''
try:
    from pulsar.apps.shell import PulsarShell
except ImportError:  # pragma nocover
    import sys
    sys.path.append('../../')
    from pulsar.apps.shell import PulsarShell


if __name__ == '__main__':  # pragma nocover
    PulsarShell().start()
