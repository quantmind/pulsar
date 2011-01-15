from datetime import timedelta

from unuk.contrib.tasks import PeriodicTask,  anchorDate

class TestPeriodicTask(PeriodicTask):
    abstract = True
    run_every = timedelta(hours=1)


class TestPeriodic(TestPeriodicTask):
        
    def run(self, task_name, task_id, logger):
        pass
    
class TestPeriodicError(TestPeriodicTask):
    
    def run(self, task_name, task_id, logger):
        raise Exception('kaputt')
    

class AnchoredEveryHour(TestPeriodicTask):
    anchor    = anchorDate(minute = 25)
    
    def run(self, task_name, task_id, logger):
        raise Exception('kaputt')
    
    
class AnchoredEvery2seconds(TestPeriodicTask):
    anchor    = anchorDate(second = 0)
    
