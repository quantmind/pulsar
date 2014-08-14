

def post_request(response, exc=None):
    '''Handle a post request, ``response`` is an instance of
    :class:`.HttpServerResponse`
    '''
    if not exc:
        logger = response.logger
        headers = response.headers_sent.decode('ISO-8859-1')
        logger.info('request done with response headers\n%s', headers)
