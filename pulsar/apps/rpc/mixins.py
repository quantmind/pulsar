from pulsar import send, make_async

from .jsonrpc import JSONRPC


__all__ = ['PulsarServerCommands']


class PulsarServerCommands(JSONRPC):
    '''Some useful commands to add to your :class:`JSONRPC` handler to get you
 started. It exposes the following functions:
 
.. method:: ping()

    Return ``pong``.
    
.. method:: server_info()

    Return a dictionary of information regarding the server and workers.
    It invokes the :meth:`extra_server_info` for adding custom information.
    
    :rtype: ``dict``.
    
.. method:: functions_list()

    Return the list of functions available in the rpc handler
    
    :rtype: ``list``.
    
.. method:: kill_actor(aid)

    :parameter aid: :attr:`pulsar.Actor.aid` of the actor to kill
'''
    
    def rpc_ping(self, request):
        '''Ping the server'''
        return 'pong'
    
    def rpc_echo(self, request, message=''):
        '''Echo the server'''
        return message
    
    def rpc_server_info(self, request):
        '''Dictionary of information about the server'''
        info = send('arbiter', 'info')
        return info.add_callback(lambda res: self.extra_server_info(
                                                       request, res))
    
    def rpc_functions_list(self, request):
        return list(self.listFunctions())
    
    def rpc_documentation(self, request):
        '''Documentation in restructured text.'''
        return self.docs()
    
    def rpc_kill_actor(self, request, aid):
        '''Kill and actor which match the id *aid*'''
        return send('arbiter', 'kill_actor', aid)
        
    def extra_server_info(self, request, info):
        '''Add additional information to the info dictionary.'''
        return info
    
