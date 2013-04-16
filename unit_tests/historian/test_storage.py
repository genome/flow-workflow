import unittest
import copy
import sqlite3
from collections import defaultdict
from flow_workflow.historian.storage import WorkflowHistorianStorage
from flow_workflow.historian import storage
from sqlalchemy.exc import IntegrityError, OperationalError, SQLAlchemyError

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
    def setUp(self):
        self.s = TestHistorianStorage(connection_string="sqlite:///:memory:",
                owner="WORKFLOW")
        self.s.create_tables()
        self.e = self.s.engine
        self.update_info = {
                'net_key': 'test_net_key',
                'operation_id': 1234,
                'name': 'test_name',
                'workflow_plan_id': 333,
        }
        self.hrows = [{
            'net_key':self.update_info['net_key'],
            'operation_id':self.update_info['operation_id'],
        }]
        self.irows = [{
            'name':self.update_info['name'],
            'workflow_plan_id':self.update_info['workflow_plan_id'],
        }]
        self.erows = [{}]

    def _test_historian(self, **kwargs):
        self._test_database_contents('workflow_historian', **kwargs)

    def _test_instance(self, **kwargs):
        self._test_database_contents('workflow_instance', **kwargs)

    def _test_execution(self, **kwargs):
        self._test_database_contents('workflow_instance_execution', **kwargs)

    def _test_database_contents(self, table_name, rows=None, **kwargs):
        engine = self.e
        result = engine.execute("SELECT * FROM workflow.%s" % table_name)
        rrows = result.fetchall()

        if rows is not None:
            self.assertEqual(len(rrows), len(rows),
                    "Table '%s' expected to have %d rows. Found %d rows instead"
                    % (table_name, len(rows), len(rrows)))
            for rrow, row in zip(rrows, rows):
                self._test_database_row(rrow, **row)
        else:
            self.assertEqual(len(rrows), 1,
                "Table '%s' expected to have 1 row. Found %d rows instead." %
                (table_name, len(rrows)))
            self._test_database_row(rrows[0], **kwargs)

    def _test_database_row(self, expected, **kwargs):
        for name, value in kwargs.items():
            uname = name.upper()
            self.assertEqual(expected[uname], value,
                "Column '%s' expected to have value '%s'. Found '%s' instead." %
                (uname, expected[uname], value))

    def test_the_test_class(self):
        self.assertEqual(1, self.s.next_execution_id(None))
        self.assertEqual(2, self.s.next_execution_id(None))

        self.assertEqual(1, self.s.next_instance_id(None))
        self.assertEqual(2, self.s.next_instance_id(None))

        self.assertEqual(3, self.s.next_instance_id(None))
        self.assertEqual(3, self.s.next_execution_id(None))

    def test_single_insert(self):
        self.s.update(self.update_info)

        self._test_historian(rows=self.hrows)
        self._test_instance(rows=self.irows)
        self._test_execution(rows=self.erows)

        # current_execution_id == workflow_execution_id
        result = self.e.execute("""
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
        self.update_info['status'] = status
        self.s.update(self.update_info)

        expected_status = status
        if second_status is not None:
            self.update_info['status'] = second_status
            self.s.update(self.update_info)
            expected_status = second_status

        self.erows[0]['status'] = expected_status
        self.erows[0]['is_done'] = is_done
        self.erows[0]['is_running']= is_running
        self._test_execution(rows=self.erows)

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
        self.s.update(self.update_info)
        self.update_info['status'] = 'done'
        self.s.update(self.update_info)

        self.erows[0]['status'] = self.update_info['status']
        self._test_instance(rows=self.irows)
        self._test_execution(rows=self.erows)

    def test_non_overwriting_status_update(self):
        self.update_info['status'] = 'done'
        self.update_info['stdout'] = '1'
        self.update_info['parallel_index'] = 9

        self.s.update(self.update_info)

        self.irows[0]['parallel_index'] = self.update_info['parallel_index']
        self.erows[0]['status'] = self.update_info['status']
        self.erows[0]['stdout'] = self.update_info['stdout']

        self._test_instance(rows=self.irows)
        self._test_execution(rows=self.erows)


        self.update_info['status'] = 'running'
        self.update_info['parallel_index'] = 0
        self.update_info['stdout'] = 'X'
        self.update_info['stderr'] = '2'

        # only updates stderr since others were not NULL and
        # running < done in STATUSES list.
        self.s.update(self.update_info)

        self._test_instance(parallel_index=9)
        self._test_execution(status='done', stdout='1', stderr='2')


        self.update_info['status'] = None

        # updates nothing because status is None
        self.s.update(self.update_info)

        self._test_instance(parallel_index=9)
        self._test_execution(status='done', stdout='1', stderr='2')

    def test_overwriting_status_update(self):
        self.update_info['status'] = 'new'
        self.update_info['stdout'] = '1'
        self.update_info['parallel_index'] = 9

        self.s.update(self.update_info)

        self._test_instance(parallel_index=9)
        self._test_execution(status='new', stdout='1')

        self.update_info['status'] = 'running'
        self.update_info['parallel_index'] = 0
        self.update_info['stdout'] = 'X'
        self.update_info['stderr'] = '2'

        # should update everything since running > new
        self.s.update(self.update_info)

        self._test_instance(parallel_index=0)
        self._test_execution(status='running', stdout='X', stderr='2')

#    def test_rollback1(self):
#        # create mock engine
#        mock_engine = Mock
#        # create
#        self.assertRaises(OperationalError, self.s.update, self.update_info)

    def test_bad_status(self):
        self.update_info['status'] = 'bad'
        self.assertRaises(ValueError, self.s.update, self.update_info)

    def test_default_insertion_values(self):
        # no 'status' in update_info
        self.s.update(self.update_info)

        self._test_execution(status='new')

    def test_double_insert(self):
        u1 = copy.copy(self.update_info)
        u1['status'] = 'new'

        u2 = copy.copy(self.update_info)
        u2['status'] = 'running'
        u2['operation_id'] = 5678

        self.s.update(u1)
        self.s.update(u2)

        rows = [
                {'status':'new'},
                {'status':'running'}
        ]
        self._test_execution(rows=rows)

    def test_recursive_update_non_subflow(self):
        self.update_info['status'] = 'running'
        self.update_info['parent_net_key'] = 'test_net_key'
        self.update_info['parent_operation_id'] = 4567
        self.update_info['is_subflow'] = False

        self.s.update(self.update_info)

        irows = [
                {'name':'test_name', 'parent_instance_id':2},
                {'name':'pending'}, # was made by recursive call
        ]
        self._test_instance(rows=irows)

        erows = [
                {'status':'running'},
                {'status':'new'},
        ]
        self._test_execution(rows=erows)

    def test_recursive_update_subflow(self):
        self.update_info['status'] = 'running'
        self.update_info['parent_net_key'] = 'test_net_key2'
        self.update_info['parent_operation_id'] = 4567
        self.update_info['is_subflow'] = True

        self.s.update(self.update_info)

        irows = [
                {'name':'test_name', 'parent_execution_id':2},
                {'name':'pending'}, # was made by recursive call
        ]
        self._test_instance(rows=irows)

        erows = [
                {'status':'running'},
                {'status':'new'},
        ]
        self._test_execution(rows=erows)

    def test_recursive_update_peer(self):
        self.update_info['status'] = 'running'
        self.update_info['peer_net_key'] = self.update_info['net_key']
        self.update_info['peer_operation_id'] = 4567

        self.s.update(self.update_info)

        rows = [
                {'peer_instance_id':2},
                {'name':'pending'},
        ]
        self._test_instance(rows=rows)

        rows = [
                {'status':'running'},
                {'status':'new'},
        ]
        self._test_execution(rows=rows)

    def test_non_recursive_update_peer(self):
        u1 = copy.copy(self.update_info)
        u1['name'] = 'peer-guy'
        u1['peer_net_key'] = u1['net_key']
        u1['peer_operation_id'] = u1['operation_id']

        u2 = copy.copy(u1)
        u2['name'] = 'another-peer-guy'
        u2['operation_id'] = 4567

        self.s.update(u1)
        self.s.update(u2)

        rows = [
                {'name':'peer-guy', 'peer_instance_id':1},
                {'name':'another-peer-guy', 'peer_instance_id':1},
        ]
        self._test_instance(rows=rows)
