'''
Importing this module add this extra settings to pulsar
'''
import pulsar

class BackendServer(pulsar.Setting):
    name = 'backend_server'
    flags = ['-s', '--backend-server']
    default = ''
    validator = pulsar.validate_string
    desc = 'Connection string to a backend server'
    