* Removed release application and created a new repository for it (https://github.com/quantmind/agile)
* ``wait`` method in ``greenio`` app accept extra parameter for checking if in child greenlet
* Specialised ``MustBeInChildGreenlet`` error for functions which should be called on a child greenlet
* Critical bug fix in ``pubsub`` subscribe method for Redis backend
