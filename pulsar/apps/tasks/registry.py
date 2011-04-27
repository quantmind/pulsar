import inspect

class TaskRegistry(dict):
    """Site registry for tasks."""

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
        """Return a generator of all tasks of a specific type."""
        return ((task_name, task)
                    for task_name, task in self.data.items()
                            if task.type == type)


registry = TaskRegistry()
