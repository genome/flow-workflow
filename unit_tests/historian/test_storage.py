import unittest
import sqlite3
from collections import defaultdict
from flow_workflow.historian.storage import WorkflowHistorianStorage, EMPTY_FROZEN_HASH

class TestHistorianStorage(WorkflowHistorianStorage):
    def __init__(self):
        self._ids = defaultdict(lambda:0)
        self._connection = None

    def _connect(self):
        conn = sqlite3.connect(":memory:")
        cur = conn.cursor()
        cur.execute(CREATE_INSTANCE_TABLE)
        cur.execute(CREATE_EXECUTION_TABLE)
        conn.commit()
        return conn

    def _next_id(self, conn, table_name):
        self._ids[table_name] += 1
        return self._ids[table_name]

CREATE_INSTANCE_TABLE = """
CREATE TABLE WORKFLOW_INSTANCE (
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
CREATE TABLE WORKFLOW_INSTANCE_EXECUTION (
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

class TestStorage(unittest.TestCase):
    def setUp(self):
        self.s = TestHistorianStorage()

    def tearDown(self):
        self.s.connection.close()

    def test_test_class(self):
        conn = self.s.connection
        self.assertEqual(1, self.s.next_execution_id(conn))
        self.assertEqual(2, self.s.next_execution_id(conn))

        self.assertEqual(1, self.s.next_instance_id(conn))
        self.assertEqual(2, self.s.next_instance_id(conn))

        self.assertEqual(3, self.s.next_instance_id(conn))
        self.assertEqual(3, self.s.next_execution_id(conn))

