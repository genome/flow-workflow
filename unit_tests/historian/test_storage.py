import unittest
import sqlite3
from collections import defaultdict
from flow_workflow.historian.storage import WorkflowHistorianStorage

class TestHistorianStorage(WorkflowHistorianStorage):
    def __init__(self, *args, **kwargs):
        WorkflowHistorianStorage.__init__(self, *args, **kwargs)
        self._ids = defaultdict(lambda:0)

    def create_tables(self):
        with self.engine.begin() as conn:
            conn.execute("ATTACH DATABASE ':memory:' as WORKFLOW")
            conn.execute(CREATE_INSTANCE_TABLE)
            conn.execute(CREATE_EXECUTION_TABLE)
            conn.execute(CREATE_WORKFLOW_HISTORIAN_TABLE)
            conn.execute(CREATE_WORKFLOW_PLAN_TABLE)

    def _next_id(self, conn, table_name):
        self._ids[table_name] += 1
        return self._ids[table_name]

CREATE_INSTANCE_TABLE = """
CREATE TABLE WORKFLOW.WORKFLOW_INSTANCE (
    WORKFLOW_INSTANCE_ID integer primary key not null,
    PARENT_INSTANCE_ID integer,
    PEER_INSTANCE_ID integer,
    CURRENT_EXECUTION_ID integer,
    WORKFLOW_PLAN_ID integer not null,
    NAME varchar not null,
    INPUT_STORED varchar,
    OUTPUT_STORED varchar,
    PARALLEL_INDEX integer,
    PARENT_EXECUTION_ID integer,
    INTENTION varchar
)
"""
CREATE_EXECUTION_TABLE = """
CREATE TABLE WORKFLOW.WORKFLOW_INSTANCE_EXECUTION (
    WORKFLOW_EXECUTION_ID integer primary key not null,
    WORKFLOW_INSTANCE_ID integer not null,
    STATUS varchar not null,
    START_TIME timestamp,
    END_TIME timestamp,
    EXIT_CODE integer,
    STDOUT varchar,
    STDERR varchar,
    IS_DONE integer,
    IS_RUNNING integer,
    DISPATCH_ID varchar,
    CPU_TIME float,
    MAX_MEMORY integer,
    MAX_SWAP integer,
    MAX_PROCESSES integer,
    MAX_THREADS integer,
    USER_NAME varchar
)
"""
CREATE_WORKFLOW_HISTORIAN_TABLE = """
CREATE TABLE WORKFLOW.WORKFLOW_HISTORIAN (
    NET_KEY varchar not null,
    OPERATION_ID integer not null,
    WORKFLOW_INSTANCE_ID integer,
    PRIMARY KEY (NET_KEY, OPERATION_ID)
)
"""
CREATE_WORKFLOW_PLAN_TABLE = """
CREATE TABLE WORKFLOW.WORKFLOW_PLAN (
    WORKFLOW_PLAN_ID integer primary key not null,
    XML varchar
)
"""

class TestStorage(unittest.TestCase):
    def _test_database_row(self, expected, **kwargs):
        for name, value in kwargs.items():
            uname = name.upper()
            self.assertEqual(expected[uname], value)

    def _test_database_contents(self, engine, table_name, rows=None, **kwargs):
        result = engine.execute("SELECT * FROM workflow.%s" % table_name)
        rrows = result.fetchall()

        if rows is not None:
            self.assertEqual(len(rrows), len(rows))
            for rrow, row in zip(rrows, rows):
                self._test_database_row(rrow, **row)
        else:
            self.assertEqual(len(rrows), 1)
            self._test_database_row(rrows[0], **kwargs)

    def setUp(self):
        self.s = TestHistorianStorage("sqlite:///:memory:", "WORKFLOW")
        self.s.create_tables()

    def test_test_class(self):
        self.assertEqual(1, self.s.next_execution_id(None))
        self.assertEqual(2, self.s.next_execution_id(None))

        self.assertEqual(1, self.s.next_instance_id(None))
        self.assertEqual(2, self.s.next_instance_id(None))

        self.assertEqual(3, self.s.next_instance_id(None))
        self.assertEqual(3, self.s.next_execution_id(None))

    def test_single_insert(self):
        engine = self.s.engine

        self.s.update("test", 1234, name='test_name', plan_id=1, status='scheduled')

        self._test_database_contents(engine, 'workflow_historian',
                net_key='test', operation_id=1234, workflow_instance_id=1)

        self._test_database_contents(engine, 'workflow_instance',
                name='test_name')

        self._test_database_contents(engine, 'workflow_instance_execution',
                status='scheduled')

        # current_execution_id == workflow_execution_id
        result = engine.execute("""
SELECT workflow_instance_execution.workflow_execution_id,
       workflow_instance.current_execution_id
FROM workflow.workflow_instance_execution
JOIN workflow.workflow_instance
ON workflow_instance_execution.workflow_instance_id=
   workflow_instance.workflow_instance_id
""")
        rows = result.fetchall()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0][0], rows[0][1])

    def _test_is_done_is_running(self, status, is_done, is_running,
            second_status=None):
        self.s.update('test', 1234, name='test_name', plan_id=1, status=status)

        expected_status = status
        if second_status is not None:
            self.s.update('test', 1234, name='test_name', plan_id=1, status=second_status)
            expected_status = second_status

        self._test_database_contents(self.s.engine, 'workflow_instance_execution',
                status=expected_status, is_done=is_done, is_running=is_running)

    def test_is_done_is_running(self):
        self._test_is_done_is_running('done', 1, 0)
        self.setUp()

        self._test_is_done_is_running('scheduled', 0, 1)
        self.setUp()

        self._test_is_done_is_running('running', 0, 1)
        self.setUp()

        self._test_is_done_is_running('crashed', 0, 0)
        self.setUp()

        self._test_is_done_is_running('new', 1, 0, second_status='done')
        self.setUp()

        self._test_is_done_is_running('new', 0, 1, second_status='scheduled')
        self.setUp()

        self._test_is_done_is_running('new', 0, 1, second_status='running')
        self.setUp()

        self._test_is_done_is_running('new', 0, 0, second_status='crashed')
        self.setUp()

    def test_update(self):
        engine = self.s.engine

        self.s.update("test", 1234, name='test_name', plan_id=1, status='new')
        self.s.update("test", 1234, name='test_name', plan_id=1, status='done')

        self._test_database_contents(engine, 'workflow_instance',
                workflow_instance_id=1, name='test_name')

        self._test_database_contents(engine, 'workflow_instance_execution',
                status='done')

    def test_non_overwriting_status_update(self):
        engine = self.s.engine

        self.s.update("test", 1234, name='test_name', plan_id=1, status='done',
                stdout='1', parallel_index=9)

        self._test_database_contents(engine, 'workflow_instance',
                workflow_instance_id=1, name='test_name', parallel_index=9)
        self._test_database_contents(engine, 'workflow_instance_execution',
                status='done', stdout='1')

        # only updates stderr since others were not NULL and
        # running < done in STATUSES list.
        self.s.update("test", 1234, name='test_name', plan_id=1, status='running',
                parallel_index=0, stdout='X', stderr='2')

        self._test_database_contents(engine, 'workflow_instance',
                parallel_index=9)
        self._test_database_contents(engine, 'workflow_instance_execution',
                status='done', stdout='1', stderr='2')

    def test_default_insertion_values(self):
        engine = self.s.engine

        self.s.update("test", 1234, name='test_name', plan_id=1)

        self._test_database_contents(engine, 'workflow_instance',
                current_execution_id=1, workflow_plan_id=1)
        self._test_database_contents(engine, 'workflow_instance_execution',
                status='new')

    def test_overwriting_status_update(self):
        engine = self.s.engine

        self.s.update("test", 1234, name='test_name', plan_id=1, status='new',
                stdout='1', parallel_index=9)

        self._test_database_contents(engine, 'workflow_instance',
                workflow_instance_id=1, name='test_name', parallel_index=9)
        self._test_database_contents(engine, 'workflow_instance_execution',
                status='new', stdout='1')

        # updates all values since running > new in STATUSES list.
        self.s.update("test", 1234, name='test_name', plan_id=1, status='running',
                parallel_index=0, stdout='X', stderr='2')

        self._test_database_contents(engine, 'workflow_instance',
                parallel_index=0)
        self._test_database_contents(engine, 'workflow_instance_execution',
                status='running', stdout='X', stderr='2')

    def test_double_insert(self):
        engine = self.s.engine

        self.s.update("test", 1234, name='test_name', plan_id=1, status='new')
        self.s.update("test", 5678, name='test_name', plan_id=1, status='running')

        rows = [
                {'status':'new'},
                {'status':'running'}
        ]
        self._test_database_contents(engine, 'workflow_instance_execution',
                rows=rows)

    def test_recursive_update_non_subflow(self):
        engine = self.s.engine

        self.s.update("test", 1234, name='test_name', plan_id=1, status='running',
                parent_net_key='test', parent_operation_id=4567,
                is_subflow=False)

        rows = [
                {'name':'test_name', 'parent_instance_id':2},
                {'name':'pending'}, # was made by recursive call
        ]
        self._test_database_contents(engine, 'workflow_instance',
                rows=rows)

        rows = [
                {'status':'running'},
                {'status':'new'},
        ]
        self._test_database_contents(engine, 'workflow_instance_execution',
                rows=rows)

    def test_recursive_update_subflow(self):
        engine = self.s.engine

        self.s.update("test", 1234, name='test_name', plan_id=1, status='running',
                parent_net_key='test', parent_operation_id=4567,
                is_subflow=True)

        rows = [
                {'name':'test_name', 'parent_execution_id':2},
                {'name':'pending'}, # was made by recursive call
        ]
        self._test_database_contents(engine, 'workflow_instance',
                rows=rows)

        rows = [
                {'status':'running'},
                {'status':'new'},
        ]
        self._test_database_contents(engine, 'workflow_instance_execution',
                rows=rows)

    def test_recursive_update_peer(self):
        engine = self.s.engine

        self.s.update("test", 1234, name='test_name', plan_id=1, status='running',
                peer_net_key='test', peer_operation_id=4567)

        rows = [
                {'name':'test_name', 'peer_instance_id':2},
                {'name':'pending'}, # was made by recursive call
        ]
        self._test_database_contents(engine, 'workflow_instance',
                rows=rows)

        rows = [
                {'status':'running'},
                {'status':'new'},
        ]
        self._test_database_contents(engine, 'workflow_instance_execution',
                rows=rows)

    def test_non_recursive_update_peer(self):
        engine = self.s.engine

        self.s.update("test", 1234, name='peer-guy', plan_id=1,
                peer_net_key='test', peer_operation_id=1234)
        self.s.update("test", 5678, name='another-peer-guy', plan_id=1,
                peer_net_key='test', peer_operation_id=1234)

        rows = [
                {'name':'peer-guy', 'peer_instance_id':1},
                {'name':'another-peer-guy', 'peer_instance_id':1},
        ]
        self._test_database_contents(engine, 'workflow_instance',
                rows=rows)

