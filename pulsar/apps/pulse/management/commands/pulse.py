# -*- coding: utf-8 -
import sys

import pulsar
from pulsar import Setting
from pulsar.utils.importer import module_attribute
from pulsar.apps.wsgi import (WSGIServer, LazyWsgi, WsgiHandler,
                              wait_for_body_middleware,
                              middleware_in_executor)

try:
    from pulsar.apps import greenio
    from pulsar.apps.greenio import pg, local
except ImportError:
    greenio = None
    pg = None

from django.core.management.base import (BaseCommand, CommandError,
                                         OutputWrapper, handle_default_options)
from django.core.wsgi import get_wsgi_application
from django.db import connections


class PulseAppName(Setting):
    section = "Django Pulse"
    app = "pulse"
    name = "pulse_app_name"
    flags = ["--pulse-app-name"]
    default = 'django_pulsar'
    desc = """\
        Name for the django pulse application
        """


class Wsgi(LazyWsgi):
    cfg = None

    def setup(self, environ=None):
        from django.conf import settings
        app = get_wsgi_application()
        green_workers = self.cfg.greenlet if self.cfg else 0
        if greenio and green_workers:
            if pg:
                pg.make_asynchronous()
            app = greenio.RunInPool(app, green_workers)
            self.green_safe_connections()
        else:
            app = middleware_in_executor(app)
        return WsgiHandler((wait_for_body_middleware, app))

    def green_safe_connections(self):
        connections._connections = local()


class Command(BaseCommand):
    #option_list = BaseCommand.option_list + (pulse_app_name,) + PULSAR_OPTIONS
    help = "Starts a fully-functional Web server using pulsar."
    args = 'pulse --help for usage'

    def handle(self, *args, **options):
        if args:
            raise CommandError('pulse --help for usage')
        app_name = options.get('pulse-app-name')
        callable = Wsgi()
        if options.pop('dryrun', False) is True:  # used for testing
            return callable
        #callable.setup()
        cfg = pulsar.Config(apps=['socket', 'pulse'],
                            server_software=pulsar.SERVER_SOFTWARE,
                            **options)
        server = WSGIServer(callable=callable, name=app_name, cfg=cfg,
                            parse_console=False)
        callable.cfg = server.cfg
        server.start()

    def get_version(self):
        return pulsar.__version__

    def create_parser(self, prog_name, subcommand):
        cfg = pulsar.Config(apps=['socket', 'pulse'],
                            exclude=['debug'],
                            description=self.help,
                            version=self.get_version(),
                            thread_workers=5)
        parser = cfg.parser()
        for option in self.option_list:
            flags = []
            if option._short_opts:
                flags.extend(option._short_opts)
            if option._long_opts:
                flags.extend(option._long_opts)
            type = option.type
            if type == 'choice':
                type = None
            s = Setting(option.dest, flags=flags, choices=option.choices,
                        default=option.default, action=option.action,
                        type=type, nargs=option.nargs, desc=option.help)
            s.add_argument(parser)
        return parser

    def run_from_argv(self, argv):
        parser = self.create_parser(argv[0], argv[1])
        options = parser.parse_args(argv[2:])
        handle_default_options(options)
        try:
            self.execute(**options.__dict__)
        except Exception as e:
            if options.traceback or not isinstance(e, CommandError):
                raise

            # self.stderr is not guaranteed to be set here
            stderr = getattr(self, 'stderr',
                             OutputWrapper(sys.stderr, self.style.ERROR))
            stderr.write('%s: %s' % (e.__class__.__name__, e))
            sys.exit(1)
