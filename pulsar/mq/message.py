from .entity import MQ, EMPTY_DICT, ACKNOWLEDGED_STATES

__all__ = ['Message']


class MessageStateError(Exception):
    pass


class Message(MQ):
    """Base class for received messages."""
    MessageStateError = MessageStateError
    
    _state = None

    #: The channel the message was received on.
    channel = None

    #: Delivery tag used to identify the message in this channel.
    delivery_tag = None

    #: Content type used to identify the type of content.
    content_type = None

    #: Content encoding used to identify the text encoding of the body.
    content_encoding = None

    #: Additional delivery information.
    delivery_info = None

    #: Message headers
    headers = None

    #: Application properties
    properties = None

    #: Raw message body (may be serialized), see :attr:`payload` instead.
    body = None

    def __init__(self, channel, body=None, delivery_tag=None,
                 content_type=None, content_encoding=None, delivery_info=None,
                 properties=None, headers=None, postencode=None,
                 **kwargs):
        self.channel = channel
        self.body = body
        self.delivery_tag = delivery_tag
        self.content_type = content_type
        self.content_encoding = content_encoding
        self.delivery_info = delivery_info or EMPTY_DICT
        self.headers = headers or EMPTY_DICT
        self.properties = properties or EMPTY_DICT
        self._decoded_cache = None
        self._state = "CREATED"
        compression = self.headers.get("compression")
        if compression:
            self.body = decompress(self.body, compression)
        if postencode and isinstance(self.body, unicode):
            self.body = self.body.encode(postencode)

    def acknowledge(self):
        """Acknowledge this message as being processed.,
        This will remove the message from the queue.

        :raises MessageStateError: If the message has already been
            acknowledged/requeued/rejected.

        """
        if self.acknowledged:
            raise self.MessageStateError(
                "Message already acknowledged with state: %s" % self._state)
        self.channel.basic_ack(self.delivery_tag)
        self._state = "ACK"

    def reject(self):
        """Reject this message.

        The message will be discarded by the server.

        :raises MessageStateError: If the message has already been
            acknowledged/requeued/rejected.

        """
        if self.acknowledged:
            raise self.MessageStateError(
                "Message already acknowledged with state: %s" % self._state)
        self.channel.basic_reject(self.delivery_tag, requeue=False)
        self._state = "REJECTED"

    def requeue(self):
        """Reject this message and put it back on the queue.

        You must not use this method as a means of selecting messages
        to process.

        :raises MessageStateError: If the message has already been
            acknowledged/requeued/rejected.

        """
        if self.acknowledged:
            raise self.MessageStateError(
                "Message already acknowledged with state: %s" % self._state)
        self.channel.basic_reject(self.delivery_tag, requeue=True)
        self._state = "REQUEUED"

    def decode(self):
        """Deserialize the message body, returning the original
        python structure sent by the publisher."""
        return serialization.decode(self.body, self.content_type,
                                    self.content_encoding)

    @property
    def acknowledged(self):
        """Set to true if the message has been acknowledged."""
        return self._state in ACKNOWLEDGED_STATES

    @property
    def payload(self):
        """The decoded message body."""
        if not self._decoded_cache:
            self._decoded_cache = self.decode()
        return self._decoded_cache



