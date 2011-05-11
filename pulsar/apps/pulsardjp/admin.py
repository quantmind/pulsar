from .models import PulsarServer, Task

NAME = 'pulsar'
ROUTE = 'pulsar'

if PulsarServer:
    from .applications import PulsarServerApplication, TasksAdmin
    admin_urls = (
                  PulsarServerApplication('/pulsar/',
                                          PulsarServer,
                                          name = 'Pulsar servers'),
                  TasksAdmin('/tasks/',Task,name = 'Tasks')
                  )