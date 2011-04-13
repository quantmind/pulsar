import inspect

UNREGISTERED_FMT = """
%s is not registered, please make sure it's imported.
""".strip()


class NotRegistered(KeyError):
    """The task is not registered."""

    def __init__(self, message, *args, **kwargs):
        message = UNREGISTERED_FMT % str(message)
        KeyError.__init__(self, message, *args, **kwargs)


class TaskRegistry(dict):
    """Site registry for tasks."""
    NotRegistered = NotRegistered

    def regular(self):
        """A generator of all regular task types."""
        return self.filter_types("regular")

    def periodic(self):
        """A generator of all periodic task types."""
        return self.filter_types("periodic")

    def register(self, task):
        """Register a task in the task registry.

        The task will be automatically instantiated if not already an
        instance.

        """
        task = inspect.isclass(task) and task() or task
        name = task.name
        self[name] = task

    def filter_types(self, type):
        """Return all tasks of a specific type."""
        
        return ((task_name, task)
                    for task_name, task in self.data.items()
                            if task.type == type)

    def __getitem__(self, key):
        try:
            return UserDict.__getitem__(self, key)
        except KeyError as exc:
            raise self.NotRegistered(exc)

    def pop(self, key, *args):
        try:
            return UserDict.pop(self, key, *args)
        except KeyError as exc:
            raise self.NotRegistered(exc)


registry = TaskRegistry()
