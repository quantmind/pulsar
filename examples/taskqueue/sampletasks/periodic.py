from datetime import timedelta

from pulsar.apps.tasks import PeriodicTask,  anchorDate

class TestPeriodicTask(PeriodicTask):
    abstract = True
    run_every = timedelta(hours=1)


class TestPeriodic(TestPeriodicTask):
        
    def __call__(self, consumer, code):
        pass
    
    
class TestPeriodicError(TestPeriodicTask):
    
    def __call__(self, consumer, code):
        raise Exception('kaputt')
    

class AnchoredEveryHour(TestPeriodicTask):
    anchor    = anchorDate(minute = 25)
    
    def __call__(self, consumer, code):
        raise Exception('kaputt')
    
    
class AnchoredEvery2seconds(TestPeriodicTask):
    anchor    = anchorDate(second = 0)
    
