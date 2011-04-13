from pulsar.apps.tasks import Task

class Ping(Task):
    def run(self,*args,**kwargs):
        return 'pong'
