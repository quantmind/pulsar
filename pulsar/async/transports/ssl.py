try:
    import ssl
    
    from .tcp import TCP

    class SSL(TCP):
        TYPE = 'SSL'
        sslcontext = ssl.SSLContext(ssl.PROTOCOL_SSLv23)
        
        def _handle_accept(self, sock, address):
            ssl_sock = self.sslcontext.wrap_socket(sock)
            self._protocol.data_received(sock, address)
            

except ImportError:  # pragma: no cover
    ssl = None