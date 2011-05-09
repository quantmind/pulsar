from stdnet import orm

from pulsar.apps.tasks import states
from pulsar.apps.tasks.consumer import TaskRequest


class Task(orm.StdModel):
    id = orm.SymbolField(primary_key = True)
    name = orm.SymbolField()
    status = orm.SymbolField()
    time_executed = orm.FloatField()
    time_start = orm.FloatField()
    time_end = orm.FloatField()
    result = orm.PickleObjectField()
    
    
class StdnetTaskRequest(TaskRequest):
    
    def _on_init(self):
        Task(id = self.id,
             name = self.name,
             time_executed = self.time_executed,
             status = states.PENDING).save()
             
    def _on_start(self,worker):
        t = Task.objects.get(id = self.id)
        t.status = states.STARTED
        t.time_start = self.time_start
        t.save()
        
    def _on_finish(self,worker):
        t = Task.objects.get(id = self.id)
        if self.exception:
            t.status = states.FAILURE
            t.result = self.exception
        else:
            t.status = states.SUCCESS
            t.result = self.result
        t.time_end = self.time_end
        t.save()
        
    @classmethod
    def get_task(cls, id, remove = False):
        try:
            task = Task.objects.get(id = id)
        except Task.DoesNotExist:
            return None
        if remove and task.time_end:
            task.delete()
        return task.todict()
    
