from pulsar import Config


def connection_made(conn):
    return conn


def post_fork(actor):
    return actor


def config(**kw):
    return Config(**kw)

foo = 5
