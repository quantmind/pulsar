import base

def app(environ, start_response):
    data = "Hello, World!\n"
    start_response("200 OK", [
        ("Content-Type", "text/plain"),
        ("Content-Length", str(len(data)))
    ])
    return iter([data])



if __name__ == '__main__':
    import sys
    sys.argv += 'wsgi1:app',
    from pulsar.apps.wsgi import run
    run()