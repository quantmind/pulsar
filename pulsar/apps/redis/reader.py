from io import BytesIO


__all__ = ['RedisReader']


REPLAY_TYPE = frozenset((b'$',  # REDIS_REPLY_STRING,
                         b'*',  # REDIS_REPLY_ARRAY,
                         b':',  # REDIS_REPLY_INTEGER,
                         b'+',  # REDIS_REPLY_STATUS,
                         b'-')) # REDIS_REPLY_ERROR

 
class redisReadTask(object):
    '''A redis read task, implemented along the line of hiredis.
    
:parameter rtype: Type of reply (one of ``$,*,:,+,-``).
:parameter response: The response to read
:parameter reader: the :class:`RedisReader` managing the :class:`redisReadTask`.
'''
    __slots__ = ('rtype','response','length','reader')
    
    def __init__(self, rtype, response, reader):
        self.reader = reader
        if rtype in REPLAY_TYPE:
            self.rtype = rtype
            length = None
            if rtype == b'-':
                if response.startswith(b'LOADING '):
                    response = b"Redis is loading data into memory"
                elif response.startswith(b'ERR '):
                    response = response[4:]
                response = self.reader.responseError(response.decode('utf-8'))
            elif rtype == b':':
                response = int(response)
            elif rtype == b'$':
                length = int(response)
                response = b''
            elif rtype == b'*':
                length = int(response)
                response = []
            self.response = response
            self.length = length
        else:
            raise self.reader.protocolError('Protocol Error.\
 Could not decode type "{0}"'.format(rtype)) 
        
    def gets(self, response = False, recursive = False):
        gets = self.reader.gets
        read = self.reader.read
        stack = self.reader._stack
        if self.rtype == b'$':
            if response is False:
                if self.length == -1:
                    return None
                response = read(self.length)
                if response is False:
                    stack.append(self)
                    return False
            self.response = response
        elif self.rtype == b'*':
            length = self.length
            if length == -1:
                return None
            stack.append(self)
            append = self.response.append
            if response is not False:
                length -= 1
                append(response)
            while length > 0:
                response = gets(True)
                if response is False:
                    self.length = length
                    return False
                length -= 1
                append(response)
            stack.pop()
        
        if stack and not recursive:
            task = stack.pop()
            return task.gets(self.response,recursive)
        
        return self.response
                             
    
class RedisReader(object):

    def __init__(self, protocolError, responseError):
        self.protocolError = protocolError
        self.responseError = responseError
        self._stack = []
        self._inbuffer = BytesIO()
    
    def read(self, length = None):
        """
        Read a line from the buffer is no length is specified,
        otherwise read ``length`` bytes. Always strip away the newlines.
        """
        if length is not None:
            chunk = self._inbuffer.read(length+2)
        else:
            chunk = self._inbuffer.readline()
        if chunk:
            if chunk[-2:] == b'\r\n':
                return chunk[:-2]
            else:
                self._inbuffer = BytesIO(chunk)
        return False
    
    def feed(self, buffer):
        buffer = self._inbuffer.read(-1) + buffer
        self._inbuffer = BytesIO(buffer)
        
    def gets(self, recursive = False):
        '''Called by the Parser'''
        if self._stack and not recursive:
            task = self._stack.pop()
        else:
            response = self.read()
            if not response:
                return False
            task = redisReadTask(response[:1], response[1:], self)
        return task.gets(recursive=recursive)
