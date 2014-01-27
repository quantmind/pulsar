import unittest

from pulsar import send

from .manage import DiningPhilosophers


class TestPhylosophers(unittest.TestCase):
    app_cfg = None
    concurrency = 'thread'

    @classmethod
    def setUpClass(cls):
        app = DiningPhilosophers(name='plato',
                                 concurrency=cls.concurrency)
        cls.app_cfg = yield send('arbiter', 'run', app)

    def test_info(self):
        while True:
            philo = []
            while len(philo) < 5:
                info = yield send('plato', 'info')
                philo = info.get('workers', [])
            all = []
            for data in philo:
                p = data.get('philosopher')
                if p:
                    all.append(p)
            if len(all) == 5:
                break

    @classmethod
    def tearDownClass(cls):
        if cls.app_cfg is not None:
            return send('arbiter', 'kill_actor', cls.app_cfg.name)
