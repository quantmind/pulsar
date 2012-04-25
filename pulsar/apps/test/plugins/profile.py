import os
import re
import shutil
import cProfile as profiler
import pstats

import pulsar
from pulsar.apps import test
from pulsar.utils.py2py3 import StringIO

words_re = re.compile( r'\s+' )
line_func = re.compile(r'(?P<line>\d+)\((?P<func>\w+)\)')
template_path = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                             'htmlfiles','profile')
headers = ('calls','totcalls','tottime','percall','cumtime',
           'percall','module')

def absolute_file(val):
    dir = os.getcwd()
    return os.path.join(dir,val)

class ProfileOption(test.TestOption):
    name = 'profile'
    flags = ["--profile"]
    action = "store_true"
    default = False
    validator = pulsar.validate_bool
    desc = '''Profile tests using the cProfile'''
    
class ProfilePathOption(test.TestOption):
    name = 'profile_stats_path'
    flags = ["--profile-stats-path"]
    default = 'htmlprof'
    desc = '''location of profile directory'''
    validator = absolute_file
    
    
def make_header(headers):
    for h in headers:
        if h:
            yield '<p>{0}</p>'.format(h)
            
def make_stat_table(data):
    yield "<thead>\n<tr>\n"
    for head in headers:
        yield '<th>'+head+'</th>'
    yield '\n</tr>\n</thead>\n<tbody>\n'
    for row in data:
        yield '<tr>\n'
        for col in row:
            yield '<td>{0}</td>'.format(col)
        yield '\n</tr>\n'
    yield '</tbody>'
        
    
def data_stream(lines, num = None):
    if num:
        lines = lines[:num]
    for line in lines:
        if not line:
            continue
        fields = words_re.split(line)
        if len(fields) == 7:
            try:
                time = float(fields[2])
            except:
                continue
            yield fields
    
    
class TestProfile(test.WrapTest):
    
    def __init__(self, test, file):
        self.file = file
        super(TestProfile,self).__init__(test)
        
    def _call(self):  
        prof = profiler.Profile()
        prof.runcall(self.testMethod)
        prof.dump_stats(self.file)
    
    
def copy_file(filename, target, context=None):
    with open(os.path.join(template_path,filename),'r') as file:
        stream = file.read()
    if context:
        stream = stream.format(context)
    with open(os.path.join(target,filename),'w') as file:
        file.write(stream)
    
    
class Profile(test.Plugin):
    active = False
    
    def configure(self, cfg):
        self.active = cfg.profile
        if self.active:
            name = cfg.profile_stats_path
            dir, fname = os.path.split(name)
            fname = '.'+fname
            self.tmp = os.path.join(dir, fname)
            self.profile_stats_path = name
        
    def getTest(self, test):
        if self.active:
            return TestProfile(test, self.tmp)
    
    def on_end(self):
        if self.active:
            out = StringIO()
            stats = pstats.Stats(self.tmp,stream=out)
            stats.sort_stats('time', 'calls')
            stats.print_stats()
            stats_str = out.getvalue()
            os.remove(self.tmp)
            stats_str = stats_str.split('\n')
            data = ''.join(make_stat_table(data_stream(stats_str[6:], 100)))
            template_file = os.path.abspath(__file__)
            if os.path.exists(self.profile_stats_path):
                shutil.rmtree(self.profile_stats_path)
            os.makedirs(self.profile_stats_path)
            for file in os.listdir(template_path):
                if file == 'index.html':
                    copy_file(file, self.profile_stats_path,
                              {'table': data,
                               'version': pulsar.__version__})
                else:
                    copy_file(file, self.profile_stats_path)
    