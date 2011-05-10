from .models import PulsarServer, Task

NAME = 'Pulsar'

if PulsarServer:
    from .applications import PulsarServerApplication, TasksAdmin
    admin_urls = (
                  PulsarServerApplication('/pulsar/',
                                          PulsarServer,
                                          name = 'Pulsar monitor'),
                  TasksAdmin('/tasks/',
                             Task,
                             name = 'Tasks',
                             list_display = ('id','name','status','time_executed',
                                             'time_start','time_end','user','api'))
            )


