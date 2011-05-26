from .models import PulsarServer, Task, Script

NAME = 'pulsar'
ROUTE = 'pulsar'

if PulsarServer:
    from .applications import PulsarServerApplication, TasksAdmin, ScriptForm,\
                                AdminApplication
    admin_urls = (
                  PulsarServerApplication('/pulsar/',
                                          PulsarServer,
                                          name = 'Pulsar servers'),
                  TasksAdmin('/tasks/',Task,name = 'Tasks'),
                  AdminApplication('/scripts/',Script,
                                   form = ScriptForm,
                                   list_display = ('name','language','parameters'),
                                   name = 'Scripting')
                  )