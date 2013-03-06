import unittest
import sqlite3
from collections import defaultdict
from flow_workflow.historian.storage import WorkflowHistorianStorage, EMPTY_FROZEN_HASH

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

    def _next_id(self, table_name):
        self._ids[table_name] += 1
        return self._ids[table_name]

CREATE_INSTANCE_TABLE = """
CREATE TABLE WORKFLOW.WORKFLOW_INSTANCE (
    WORKFLOW_INSTANCE_ID integer primary key not null,
    PARENT_INSTANCE_ID integer,
    PEER_INSTANCE_ID integer,
    CURRENT_EXECUTION_ID integer,
    WORKFLOW_PLAN_ID integer,
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
    WORKFLOW_INSTANCE_ID integer,
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



class TestStorage(unittest.TestCase):
    def setUp(self):
        self.s = TestHistorianStorage("sqlite:///:memory:", "WORKFLOW")
        self.s.create_tables()

    def test_test_class(self):
        self.assertEqual(1, self.s.next_execution_id())
        self.assertEqual(2, self.s.next_execution_id())

        self.assertEqual(1, self.s.next_instance_id())
        self.assertEqual(2, self.s.next_instance_id())

        self.assertEqual(3, self.s.next_instance_id())
        self.assertEqual(3, self.s.next_execution_id())

    def test_single_insert(self):
        engine = self.s.engine

        self.s.update("test", 1234, name='test_name')
        result = engine.execute("SELECT workflow_instance_id, name FROM workflow.workflow_instance")
        rows = result.fetchall()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]['NAME'], 'test_name')


