from djpcms import forms, html
from djpcms.forms.layout import uniforms as uni

__all__ = ['get_job_form','register_job_form','ServerForm']


job_forms = {}


class ServerForm(forms.Form):
    code = forms.CharField()
    path = forms.CharField(initial = 'http://127.0.0.1')
    notes = forms.CharField(widget = html.TextArea,
                            required = False)
    location = forms.CharField(required = False)
    
    
EmptyJobRunForm = forms.HtmlForm(
    forms.Form,
    inputs = (('run','run'),),
    layout = uni.Layout(default_style = uni.blockLabels2)
)


def register_job_form(job,form):
    '''Register a form for lunch a new task from a job specification.'''
    job_forms[job] = form


def get_job_form(instance):
    if instance and instance.id in job_forms:
        return job_forms[instance.id]
    else:
        return EmptyJobRunForm
    