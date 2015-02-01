# Flask running in pulsar
```
python server.py
```

# Flask running standalone
```
python routes.py
```

# Issues
## Pulsar wsgi logging access requests
When the app starts up in pulsar, no access requests are logged.

```
curl http://localhost:8080/
```
Output after request
```
(flask)[rgil@rem5-3 flask]$ python server.py 
{'pulsar.wsgi': <logging.Logger object at 0x104849e90>, 'trollius': <logging.Logger object at 0x1044f3fd0>, 'concurrent': <logging.PlaceHolder object at 0x104502bd0>, 'pulsar.config': <logging.Logger object at 0x104485950>, 'pulsar': <logging.Logger object at 0x104550710>, 'concurrent.futures': <logging.Logger object at 0x104502b90>}
13:56:59 [p=35097, t=140735153853200, INFO, pulsar.arbiter] mailbox serving on 127.0.0.1:53979
13:56:59 [p=35097, t=140735153853200, INFO, pulsar.arbiter] started
13:56:59 [p=35100, t=140735153853200, INFO, pulsar.wsgi.worker] wsgi serving on 0.0.0.0:8080
13:56:59 [p=35101, t=140735153853200, INFO, pulsar.wsgi.worker] wsgi serving on 0.0.0.0:8080
13:56:59 [p=35097, t=140735153853200, INFO, pulsar.wsgi] started
13:56:59 [p=35102, t=140735153853200, INFO, pulsar.wsgi.worker] wsgi serving on 0.0.0.0:8080
13:56:59 [p=35100, t=140735153853200, INFO, pulsar.wsgi.worker] started
13:56:59 [p=35101, t=140735153853200, INFO, pulsar.wsgi.worker] started
13:56:59 [p=35102, t=140735153853200, INFO, pulsar.wsgi.worker] started
13:56:59 [p=35103, t=140735153853200, INFO, pulsar.wsgi.worker] wsgi serving on 0.0.0.0:8080
13:56:59 [p=35103, t=140735153853200, INFO, pulsar.wsgi.worker] started
```

## Running standalone dev mode in flask
Logs are displayed. Originating from werkzeug afaict.

Request
```
curl http://localhost:5000/
```

```
(flask)[rgil@rem5-3 flask]$ python routes.py
{}
 * Running on http://127.0.0.1:5000/
127.0.0.1 - - [01/Feb/2015 13:54:34] "GET / HTTP/1.1" 200 -
```
