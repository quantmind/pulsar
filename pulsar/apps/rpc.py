from pulsar.http import rpc
from pulsar.utils.py2py3 import iteritems


class PulsarServerCommands(rpc.JSONRPC):
    '''Some useful commands to use as plugin into your rpc server'''
    
    def rpc_ping(self, request):
        '''Ping the server'''
        return 'pong'
    
    def rpc_server_info(self, request, full = False):
        '''Dictionary of information about the server'''
        worker = request.environ['pulsar.worker']
        info = worker.proxy.info(worker.arbiter, full = full)
        return info.add_callback(lambda res : self.extra_server_info(
                                                        request, res))
    
    def rpc_functions_list(self, request):
        return list(self.listFunctions())
    
    def extra_server_info(self, request, info):
        return info
    
