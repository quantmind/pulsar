import pulsar
from pulsar




def run():
    wsgi = pulsar.require('wsgi')
    task = pulsar.require('task')
    wsgi.createServer(callable = hello).run()

if __name__ == '__main__':
    '''To run this example simply type::
    
        python manage.py -c config.py
    '''
    tasks.run()