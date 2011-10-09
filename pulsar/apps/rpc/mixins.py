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
        actor = request.environ['pulsar.actor']
        info = actor.arbiter.send(actor,'info',full=full)
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
        actor = request.environ['pulsar.actor']
        actor.arbiter.send(actor,'kill_actor',aid)
        
    def extra_server_info(self, request, info):
        '''Add additional information to the info dictionary.'''
        return info
    
