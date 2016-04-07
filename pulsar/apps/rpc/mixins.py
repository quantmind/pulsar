import pulsar
from pulsar import isawaitable

from .jsonrpc import JSONRPC


__all__ = ['PulsarServerCommands']


class PulsarServerCommands(JSONRPC):
    '''Useful commands to add to your :class:`.JSONRPC` handler.

    It exposes the following functions:'''
    def rpc_ping(self, request):
        '''Ping the server.'''
        return 'pong'

    def rpc_echo(self, request, message=''):
        '''Echo the server.'''
        return message

    async def rpc_server_info(self, request):
        '''Return a dictionary of information regarding the server and workers.

        It invokes the :meth:`extra_server_info` for adding custom
        information.
        '''
        info = await pulsar.send('arbiter', 'info')
        info = self.extra_server_info(request, info)
        if isawaitable(info):
            info = await info
        return info

    def rpc_functions_list(self, request):
        '''List of (method name, method document) pair of all method exposed
        by this :class:`.JSONRPC` handler.'''
        return list(self.listFunctions())

    def rpc_documentation(self, request):
        '''Documentation in restructured text.'''
        return self.docs()

    def rpc_kill_actor(self, request, aid):
        '''Kill the actor with id equal to *aid*.'''
        return pulsar.send('arbiter', 'kill_actor', aid)

    def extra_server_info(self, request, info):
        '''An internal method.

        Used by the :meth:`rpc_server_info` method to add additional
        information to the info dictionary.
        '''
        return info
