
if __name__ == '__main__':
    import sys
    import pickle
    from multiprocessing import current_process
    from multiprocessing.spawn import import_main_path

    from pulsar.async.concurrency import run_actor

    data = pickle.load(sys.stdin.buffer)
    current_process().authkey = data['authkey']
    sys.path = data['path']
    import_main_path(data['main'])
    impl = pickle.loads(data['impl'])
    run_actor(impl)
