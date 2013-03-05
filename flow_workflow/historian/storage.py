import cx_Oracle
import logging

LOG = logging.getLogger(__name__)
EMPTY_FROZEN_HASH = "".join([chr(x) for x in [5,7,3,0,0,0,0]])

class WorkflowHistorianStorage(object):
    def __init__(self, user, password, dsn, owner):
        self.dsn = dsn
        self.user = user
        self.password = password
        self.owner = owner
        self._connection = None

    def _next_id(self, conn, table_name):
        cur = conn.cursor()
        s = "SELECT %s.%s_seq.nextval FROM DUAL" % (self.owner, table_name)
        LOG.debug("EXECUTING: %s", s)
        cur.execute(s)
        return cur.fetchone()[0]

    def next_instance_id(self, conn):
        return self._next_id(conn, 'workflow_instance')

    def next_execution_id(self, conn):
        return self._next_id(conn, 'workflow_execution')

    def update(self, net_key, operation_id,
            name=None,
            status=None,
            parent_key=None,
            is_subflow=False,
            peer_key=None,
            parallel_index=None,
            start_time=None,
            end_time=None,
            stdout=None,
            stderr=None,
            exit_code=None):

        # TODO 
        cur = self.connection.cursor()
        pass

    @property
    def connection(self):
        if self._connection is None:
            self._connection = self._connect()
        return self._connection

    def _connect(self):
        LOG.debug("Connecting to %s as user: %s",
                self.dsn, self.user)
        conn = cx_Oracle.connect(self.user, self.password, self.dsn)
        conn.autocommit = False
        return conn

