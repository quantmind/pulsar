from asyncio import Task, get_event_loop
from contextlib import contextmanager


class TaskContext:
    _previous_task_factory = None

    def __call__(self, loop, coro):
        current = Task.current_task(loop=loop)
        task = Task(coro, loop=loop)
        try:
            task._context = current._context.copy()
        except AttributeError:
            pass
        try:
            task._context_stack = current._context_stack.copy()
        except AttributeError:
            pass
        return task

    def setup(self):
        loop = get_event_loop()
        self._previous_task_factory = loop.get_task_factory()
        loop.set_task_factory(self)

    def remove(self):
        loop = get_event_loop()
        loop.set_task_factory(self._previous_task_factory)

    @contextmanager
    def begin(self, *args, **kwargs):
        for key, value in mapping(*args, **kwargs):
            self.stack_push(key, value)
        try:
            yield self
        finally:
            for key, _ in mapping(*args, **kwargs):
                self.stack_pop(key)

    def set(self, key, value):
        """Set a value in the task context
        """
        task = Task.current_task()
        try:
            context = task._context
        except AttributeError:
            task._context = context = {}
        context[key] = value

    def get(self, key):
        task = Task.current_task()
        try:
            context = task._context
        except AttributeError:
            return
        return context.get(key)

    def pop(self, key):
        context = Task.current_task()._context
        return context.pop(key)

    def stack_push(self, key, value):
        """Set a value in a task context stack
        """
        task = Task.current_task()
        try:
            context = task._context_stack
        except AttributeError:
            task._context_stack = context = {}
        if key not in context:
            context[key] = []
        context[key].append(value)

    def stack_get(self, key):
        """Set a value in a task context stack
        """
        task = Task.current_task()
        try:
            context = task._context_stack
        except AttributeError:
            return
        if key in context:
            return context[key][-1]

    def stack_pop(self, key):
        """Remove a value in a task context stack
        """
        task = Task.current_task()
        try:
            context = task._context_stack
        except AttributeError:
            raise KeyError('pop from empty stack') from None
        value = context[key]
        stack_value = value.pop()
        if not value:
            context.pop(key)
        return stack_value


def mapping(*args, **kwargs):
    if args:
        if len(args) > 1:
            raise TypeError('expected at most 1 arguments, got %d' % len(args))
        for key, value in args[0].items():
            yield key, value
    for key, value in kwargs.items():
        yield key, value
