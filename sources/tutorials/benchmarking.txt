
===================
Benchmarking
===================


Test Concurrency
======================

The simpliest way to benchmark pulsar is to use the :ref:`HttpBin <tutorials-httpbin>`
example application. It is a web server responding to several urls::

    python manage.py -b :9060


Test streaming
------------------

The ``stream`` url simulates a streaming response of chunks of length given by the
first url parameter repeated by the second url parameter.

For example, this measure the download speed when streaming chunks of 4KB 1,048,576 times
for a total of 4GB of data::

    curl http://localhost:9060/stream/4096/1048576 > /dev/null
      % Total    % Received % Xferd  Average Speed   Time    Time     Time  Current
                                     Dload  Upload   Total   Spent    Left  Speed
    100 4096M    0 4096M    0     0  87.8M      0 --:--:--  0:00:46 --:--:-- 73.1M

The download speed is relatively slow considering the test is on localhost.
However, when switching the two numbers::

    curl http://localhost:9060/stream/1048576/4096 > /dev/null
      % Total    % Received % Xferd  Average Speed   Time    Time     Time  Current
                                     Dload  Upload   Total   Spent    Left  Speed
    100 4096M    0 4096M    0     0   658M      0 --:--:--  0:00:06 --:--:--  673M

We have a download speed almost 10 times higher. Why?

Because the iterator send large chunks to pulsar and the buffering is handled
by asyncio rather than the iterator and therefore it is much more efficient.
In other words the bigger the chunks the faster the transfer rate and
the more responsive (in term of concurrent connections) the server will be.


Benchmark with JMeter
--------------------------

Install jmeter_, on a mac simply use ``homebrew``

  brew install jmeter

launch it, open the ``httpbin.jmx`` benchmark file and run it.


.. _jmeter: http://jmeter.apache.org/

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