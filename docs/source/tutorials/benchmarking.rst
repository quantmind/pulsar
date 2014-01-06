
===================
Benchmarking
===================


Test slow clients
======================

Testing pulsar with slowloris_ is a great way to check the performance under
low bandwidth yet greedy clients.

check the file limits with::

    ulimit -a

and set::

    ulimit -n 1000

Check slowloris_ usage with::

    perldoc slowloris.pl

To test for slow bandwidth greedy clients::

    ./slowloris.pl -dns www.example.com -port 80 -timeout 30 -num 1000

.. _slowloris: http://ha.ckers.org/slowloris/


Pulsar store versus Redis
============================

Benchmarking the pulsar store application versus redis is useful for
measuring the speed of the python event loop and parsing with respect
a superfast C implementation like redis.

First lunch the pulsar ds example::

    python manage.py

Than use the ``redis-benchmark`` executable, for example::

    ./redis-benchmark -t set -n 1000000 -d 20 -c 100 -p 6410

Test the set command with 100 concurrent clients with a payload of 20 bytes,
1 million times.

Unsurprisingly, running the server with pypy will deliver much faster
(about four times) results.