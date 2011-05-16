from optparse import make_option

from djpcms.apps.management.base import BaseCommand
from djpcms.core.exceptions import CommandError

import pulsar
from pulsar.apps.pulsardjp import DjpCmsApplicationCommand


def make_options():
    g_settings = pulsar.make_settings(ignore=('version',))

    keys = g_settings.keys()
    def sorter(k):
        return (g_settings[k].section, g_settings[k].order)

    opts = []
    
    for k in keys:
        setting = g_settings[k]
        if not setting.cli:
            continue

        args = tuple(setting.cli)

        kwargs = {
            "dest": setting.name,
            "metavar": setting.meta or None,
            "action": setting.action or "store",
            "type": setting.type or "string",
            "default": None,
            "help": "%s [%s]" % (setting.short, setting.default)
        }
        if kwargs["action"] != "store":
            kwargs.pop("type")

        opts.append(make_option(*args, **kwargs))

    return tuple(opts)

PULSAR_OPTIONS = make_options()


class Command(BaseCommand):
    option_list = BaseCommand.option_list + PULSAR_OPTIONS
    help = "Starts a fully-functional Web server using pulsar."
    args = '[optional port number]'
    
    def handle(self, callable, addrport=None, *args, **options):
        if args:
            raise CommandError('Usage is run_pulsar %s' % self.args)
        
        if addrport:
            options['bind'] = addrport
        
        DjpCmsApplicationCommand(callable = callable,
                                 parse_console = False,
                                 **options).start()
