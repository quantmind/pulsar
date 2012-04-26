import os
import re
import shutil
import tempfile
import cProfile as profiler
import pstats

import pulsar
from pulsar.apps import test
from pulsar.utils.py2py3 import StringIO

other_filename = 'unknown'
line_func = re.compile(r'(?P<line>\d+)\((?P<func>\w+)\)')
template_path = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                             'htmlfiles','profile')
headers = (
    ('ncalls',
     'total number of calls'),
    ('primitive calls',
     'Number primitive calls (calls not induced via recursion)'),
    ('tottime',
     'Total time spent in the given function (excluding time spent '\
     'in calls to sub-functions'),
    ('percall',
     'tottime over ncalls, the time spent by each call'),
    ('cumtime',
     'Total time spent in the given function including all subfunctions'),
    ('percall',
     'cumtime over primitive calls'),
    ('function',''),
    ('lineno',''),
    ('filename','')
    )

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
    for head, description in headers:
        yield '<th title="{1}">{0}</th>'.format(head, description)
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
        fields = [field for field in line.split() if field is not '']
        if len(fields) == 6:
            valid = True
            new_fields = fields[0].split('/')
            if len(new_fields) == 1:
                new_fields.append(new_fields[0])
            for f in fields[1:-1]:
                try:
                    float(f)
                except:
                    valid = False
                    break
                new_fields.append(f)
            if not valid:
                continue
            filenames = fields[-1].split(':')
            linefunc = filenames.pop()
            match = line_func.match(linefunc)
            if match:
                lineno, func = match.groups()
                filename = ':'.join(filenames)
                filename = filename.replace('\\','/')
                new_fields.extend((func, lineno, filename))
            else:
                new_fields.extend(('', '', other_filename))
            yield new_fields
    
    
class TestProfile(test.WrapTest):
    
    def __init__(self, test, dir):
        self.dir = dir
        super(TestProfile,self).__init__(test)
    
    def _call(self):
        prof = profiler.Profile()
        tmp = tempfile.mktemp(dir=self.dir)
        prof.runcall(self.testMethod)
        prof.dump_stats(tmp)
    
    
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
        self.profile_stats_path = cfg.profile_stats_path
        dir, name = os.path.split(self.profile_stats_path)
        fname = '.'+name
        self.profile_temp_path = os.path.join(dir, fname)
        
    def getTest(self, test):
        if self.active:
            return TestProfile(test, self.profile_temp_path)
    
    def on_start(self):
        if self.active:
            self.remove_dir(self.profile_temp_path, build=True)
        
    def remove_dir(self, dir, build=False):
        if os.path.exists(dir):
            shutil.rmtree(dir)
        if build:
            os.makedirs(dir)
        
    def on_end(self):
        if self.active:
            files = [os.path.join(self.profile_temp_path,file) for file in\
                     os.listdir(self.profile_temp_path)]
            stats = pstats.Stats(*files,**{'stream':StringIO()})
            stats.sort_stats('time', 'calls')
            stats.print_stats()
            stats_str = stats.stream.getvalue()
            self.remove_dir(self.profile_temp_path)
            stats_str = stats_str.split('\n')
            run_info = ''
            for n, line in enumerate(stats_str):
                b = 0
                while b < len(line) and line[b] == ' ':
                    b += 1
                line = line[b:]
                if line:
                    if line.startswith('ncalls'):
                        break
                    bits = line.split(' ')
                    try:
                        ncalls = int(bits[0])
                    except:
                        continue
                    else:
                        run_info = line
            data = ''.join(make_stat_table(data_stream(stats_str[n+1:], 100)))
            self.remove_dir(self.profile_stats_path, build=True)
            for file in os.listdir(template_path):
                if file == 'index.html':
                    copy_file(file, self.profile_stats_path,
                              {'table': data,
                               'run_info': run_info,
                               'version': pulsar.__version__})
                else:
                    copy_file(file, self.profile_stats_path)
    