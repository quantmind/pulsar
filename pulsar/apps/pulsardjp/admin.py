from .models import PulsarServer

if PulsarServer:
    from .applications import PulsarServerApplication
    admin_urls = (
                  PulsarServerApplication('/pulsar/',
                                          PulsarServer,
                                          name = 'Pulsar monitor',
                                          list_display = ['host','port','notes']),
            )


