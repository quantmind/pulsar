AMQP_PROTOCOL = '1.0.0'

AMQP_PROTOCOL_BYTES = 'AMQP0{0}'.format(AMQP_PROTOCOL.replace('.','')).encode('utf-8')
