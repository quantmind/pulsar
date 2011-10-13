from pulsar.apps.tasks import Job

from .forms import html, forms, uni, register_job_form

class CodeForm(forms.Form):
    code = forms.CharField(widget = html.TextArea(
                                    rows = 20,
                                    default_class = 'taboverride code'))


HtmlCodeForm = forms.HtmlForm(
    CodeForm,
    inputs = (('run','run'),),
    layout = uni.Layout(default_style = uni.blockLabels2)
)


class RunPyCode(Job):
    '''Run a python script in the task queue. The code must have a callable
named "task_function".'''
    def __call__(self, consumer, code = None, **kwargs):
        code_local = compile(code, '<string>', 'exec')
        ns = {}
        exec(code_local,ns)
        func = ns.get('task_function')
        if hasattr(func,'__call__'):
            return func(**kwargs)
        else:
            raise ValueError('task_function is not defined in script, or it\
 is not a callable')
    