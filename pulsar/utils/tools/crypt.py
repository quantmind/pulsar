from uuid import uuid4

__all__ = ['gen_unique_id']

def gen_unique_id():
    return str(uuid4())