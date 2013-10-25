
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
