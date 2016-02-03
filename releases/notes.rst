* Removed release application and created a new repository for it (https://github.com/quantmind/agile)
* ``wait`` method in ``greenio`` app accept extra parameter for checking if in child greenlet
* Specialised ``MustBeInChildGreenlet`` error for functions which should be called on a child greenlet
* Critical bug fix in ``pubsub`` subscribe method for Redis backend
* Added an asynchronous ``wsgi.file_wrapper`` to the WSGI environment [[d31e8cf](https://github.com/quantmind/pulsar/commit/d31e8cfd082431d70f9553295a6341a94d4cd19c))
* Added ``file_response`` utility to serve local files
* Introduced ``pulsar.ensure_future`` in place of ``pulsar.async``
