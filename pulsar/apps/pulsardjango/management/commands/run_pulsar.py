from optparse import make_option

import django
from django.core.management.base import BaseCommand, CommandError
from django.conf import settings
from django.utils import translation

import pulsar
from pulsar.apps.pulsardjango import DjangoApplicationCommand


PULSAR_OPTIONS = pulsar.make_options()


class Command(BaseCommand):
    option_list = BaseCommand.option_list + PULSAR_OPTIONS
    help = "Starts a fully-functional django web server using pulsar."
    args = '[optional port number]'
    
    def handle(self, addrport=None, *args, **options):
        if args:
            raise CommandError('Usage is runserver %s' % self.args)
            
        if addrport:
            options['bind'] = addrport
        
        options['default_proc_name'] = settings.SETTINGS_MODULE

        admin_media_path = options.pop('admin_media_path', '')
        quit_command = (sys.platform == 'win32') and 'CTRL-BREAK' or 'CONTROL-C'

        print("Validating models...")
        self.validate(display_num_errors=True)
        print("\nDjango version %s, using settings %r" % (django.get_version(), 
                                            settings.SETTINGS_MODULE))
        print("Server is running")
        print("Quit the server with %s." % quit_command)
 
        # django.core.management.base forces the locale to en-us.
        translation.activate(settings.LANGUAGE_CODE)
        DjangoApplicationCommand(options, admin_media_path).start()
