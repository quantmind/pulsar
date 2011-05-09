from pulsar.http import rpc


class PulsarServerCommands(rpc.JSONRPC):
    
    def rpc_ping(self, request):
        '''Ping the server'''
        return 'pong'
    
    def rpc_server_info(self, request):
        '''Dictionary of information about the server'''
        worker = request.environ['pulsar.worker']
        info = worker.proxy.info(worker.arbiter)
        return info.add_callback(lambda res : self.extra_server_info(request, res))
    
    def rpc_functions_list(self, request):
        return list(self.listFunctions())
    
    def extra_server_info(self, request, info):
        return info
    