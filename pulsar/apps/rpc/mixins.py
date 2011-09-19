from pulsar.utils.py2py3 import iteritems

from .jsonrpc import JSONRPC


__all__ = ['PulsarServerCommands']


class PulsarServerCommands(JSONRPC):
    '''Some useful commands to add to your JSONRPC server to get you started.'''
    
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
    
    def rpc_documentation(self, request):
        '''Documentation in restructured text.'''
        return self.docs()
    
    def rpc_kill_actor(self, request, aid):
        '''Kill and actor which match the id *aid*'''
        # get the worker serving the request
        worker = request.environ['pulsar.worker']
        return worker.proxy.kill_actor(worker.arbiter, aid)
        
    def rpc_shut_down(self, request):
        request.environ['pulsar.worker'].shut_down()
        
    def extra_server_info(self, request, info):
        '''Add additional information to the info dictionary.'''
        return info
    
