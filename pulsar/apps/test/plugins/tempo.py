import pulsar
from pulsar import to_string


def validate_posint_list(val):
    if not isinstance(val,(list,tuple)):
        val = to_string(val).split()
    pi = pulsar.validate_pos_int
    return [pi(v) for v in val]


class TempoScript(pulsar.Setting):
    name = "tempo_script"
    section = "Temp app"
    meta = "STRING"
    cli = ["--tempo-script"]
    validator = pulsar.validate_string
    default = 'pulsar.apps.tempo.basescript'
    desc = """\
        Script which run the benchmarking requests
        """
        
        
class TempoRPS(pulsar.Setting):
    name = "request_per_second"
    section = "Tempo Application"
    meta = "STRING"
    cli = ["--rps"]
    validator = validate_posint_list
    default = 10
    desc = """List of requests per seconds to perform, space separated.
    
For example::

    10 20 50 100 200 500
        """
        

class TotalTempoRequest(pulsar.Setting):
    name = "total_tempo_requests"
    section = "Temp app"
    meta = 'INT'
    cli = ["--ttr"]
    validator = pulsar.validate_pos_int
    default = 500
    desc = """Total number of requests for every request per second test.
        """
