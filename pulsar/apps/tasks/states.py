SUCCESS = "SUCCESS"
FAILURE = "FAILURE"
UNKNOWN = "UNKNOWN"
REVOKED = "REVOKED"
STARTED = "STARTED"
RETRY = "RETRY"
QUEUED = "QUEUED"
PENDING = "PENDING"
#: Lower index means higher precedence.
PRECEDENCE = (
              (SUCCESS, 1),
              (FAILURE, 2),
              (UNKNOWN, 3),
              (REVOKED, 4),
              (STARTED, 5),
              (QUEUED, 6),
              (RETRY, 7),
              (PENDING, 8)
              )

PRECEDENCE_MAPPING = dict(PRECEDENCE)
UNKNOWN_STATE = PRECEDENCE_MAPPING[UNKNOWN]
FULL_RUN_STATES = frozenset([SUCCESS, FAILURE])
READY_STATES = frozenset([SUCCESS, FAILURE, REVOKED])
NOT_STARTED_STATES = frozenset([QUEUED, PENDING, RETRY])
UNREADY_STATES = frozenset([QUEUED, PENDING, STARTED, RETRY])
EXCEPTION_STATES = frozenset([RETRY, FAILURE, REVOKED])
PROPAGATE_STATES = frozenset([FAILURE, REVOKED])
ALL_STATES = frozenset([QUEUED, PENDING, STARTED, SUCCESS, FAILURE,
                        RETRY, REVOKED])
status_code = lambda code: PRECEDENCE_MAPPING.get(code)