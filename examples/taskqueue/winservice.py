import os
import sys
p = lambda x: os.path.split(x)[0]
path = p(p(p(os.path.abspath(__file__))))
sys.path.insert(0, path)

import pulsar
from pulsar.utils.system import winservice

from manage import server


class TasqueueService(winservice.PulsarService):
    _svc_name_ = 'TASKSQUEUE'
    _svc_display_name_ = "PULSAR TASKSQUEUE server"
    _svc_description_ = "Pulsar asynchronous task queue server"

    def setup(self):
        server(parse_console=False, concurrency='process')


if __name__ == '__main__':
    TasqueueService.run()
