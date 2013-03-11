from sqlalchemy import create_engine
import logging
from sqlalchemy.exc import IntegrityError
from collections import defaultdict
import re

LOG = logging.getLogger(__name__)
EMPTY_FROZEN_HASH = "".join([chr(x) for x in [5,7,3,0,0,0,0]])

INSERT_INTO_WORKFLOW_HISTORIAN = """
INSERT INTO %s.workflow_historian (net_key, operation_id, workflow_instance_id)
VALUES (:net_key, :operation_id, :workflow_instance_id)
"""

SELECT_INSTANCE_ID = """
SELECT workflow_instance_id FROM %s.workflow_historian WHERE
net_key=:net_key AND operation_id=:operation_id
"""

SELECT_EXECUTION_ID = """
SELECT current_execution_id FROM %s.workflow_instance WHERE
workflow_instance_id=:workflow_instance_id
"""

SELECT_INSTANCE = """
SELECT * from %s.workflow_instance
WHERE workflow_instance_id = :workflow_instance_id
"""

SELECT_EXECUTION = """
SELECT * from %s.workflow_instance_execution
WHERE workflow_execution_id = :workflow_execution_id
"""

SELECT_STATUS = """
SELECT status FROM %s.workflow_instance_execution WHERE
workflow_execution_id = :workflow_execution_id
"""

UPDATE = """
UPDATE %s SET %s
WHERE %s=:%s
"""

STATUSES = [
        'new',
        'scheduled',
        'running',
        'failed',
        'crashed',
        'done',
]

def execute_and_log(engine, stmnt, **kwargs):
    full_stmnt = stmnt
    for name, value in kwargs.items():
        full_stmnt = re.sub(r':' + name, str(value), full_stmnt)
    LOG.debug("EXECUTING: %s", full_stmnt)
    return engine.execute(stmnt, **kwargs)

class WorkflowHistorianStorage(object):
    def __init__(self, connection_string, owner):
        self.connection_string = connection_string
        self.owner = owner
        self.engine = create_engine(connection_string)

    def _next_id(self, engine, table_name):
        stmnt = "SELECT %s.%s_seq.nextval FROM DUAL" % (self.owner, table_name)
        result = execute_and_log(engine, stmnt)
        return result.fetchone()[0]

    def next_instance_id(self, engine):
        return self._next_id(engine, 'workflow_instance')

    def next_execution_id(self, engine):
        return self._next_id(engine, 'workflow_execution')

    def next_plan_id(self, engine):
        return self._next_id(engine, 'workflow_plan')

    def update(self, net_key, operation_id, name,
            parent_net_key      = None,
            parent_operation_id = None,
            is_subflow          = False,
            peer_net_key        = None,
            peer_operation_id   = None,
            parallel_index      = None,
            status              = None,
            user_name           = None,
            dispatch_id         = None,
            start_time          = None,
            end_time            = None,
            stdout              = None,
            stderr              = None,
            exit_code           = None,
            conn                = None,
            trans               = None,
            recursion_level     = 0):

        if recursion_level > 1:
            raise RuntimeError("update should never recurse more than once!");

        if status is not None:
            try:
                STATUSES.index(status)
            except ValueError:
                raise ValueError("Status must be one of %s, not '%s'" %
                        (STATUSES, status))

        # only the update call that created this transaction should commit
        created_transaction = False
        if conn is None:
            conn = self.engine.connect()
            trans = conn.begin()
            created_transaction = True

        try:
            execute_and_log(conn,
                    INSERT_INTO_WORKFLOW_HISTORIAN % self.owner,
                    net_key=net_key,
                    operation_id=operation_id,
                    workflow_instance_id=instance_id)
        except IntegrityError:
            LOG.debug("Failed to insert (net_key=%s, operation_id=%s) into "
                    "workflow_historian table, attempting update instead." %
                    (net_key, operation_id))
            try:
                instance_id = self._get_instance_id(conn, trans, net_key,
                        operation_id)
                instance_row, execution_row = self._get_rows(conn, trans,
                        instance_id)
                execution_id = instance_row['CURRENT_EXECUTION_ID']
                should_overwrite = self._should_overwrite(
                        execution_row['STATUS'], status)

                update_instance_dict = self._get_update_instance_dict(conn,
                        trans, name,
                        instance_row        = instance_row,
                        should_overwrite    = should_overwrite,
                        parent_net_key      = parent_net_key,
                        parent_operation_id = parent_operation_id,
                        is_subflow          = is_subflow,
                        peer_net_key        = peer_net_key,
                        peer_operation_id   = peer_operation_id,
                        parallel_index      = parallel_index)

                # update instance
                _perform_update(conn, update_instance_dict,
                        "%s.workflow_instance" % self.owner,
                        "workflow_instance_id", instance_id)

                update_execution_dict = self._get_update_execution_dict(conn,
                        trans,
                        execution_row    = execution_row,
                        should_overwrite = should_overwrite,
                        status           = status,
                        dispatch_id      = dispatch_id,
                        user_name        = user_name,
                        start_time       = start_time,
                        end_time         = end_time,
                        stdout           = stdout,
                        stderr           = stderr,
                        exit_code        = exit_code)

                # update instance_execution
                _perform_update(conn, update_execution_dict,
                        "%s.workflow_instance_execution" % self.owner,
                        "workflow_execution_id", execution_id)

                if created_transaction:
                    trans.commit()
            except:
                trans.rollback()
                raise
        except:
            trans.rollback()
            raise
        else:
            try:

                # insert NULL into plan
                plan_id = self.next_plan_id(conn)
                _perform_insert(conn, {'WORKFLOW_PLAN_ID':plan_id},
                        table_name='%s.workflow_plan' % self.owner)

                # insert into instance
                instance_id = self.next_instance_id(conn)
                execution_id = self.next_execution_id(conn)
                insert_instance_dict = {
                        'WORKFLOW_INSTANCE_ID': instance_id,
                        'WORKFLOW_PLAN_ID': plan_id,
                        'CURRENT_EXECUTION_ID': execution_id,
                        'INPUT_STORED': EMPTY_FROZEN_HASH,
                        'OUTPUT_STORED': EMPTY_FROZEN_HASH,
                }

                insert_instance_dict.update(self._get_update_instance_dict(conn,
                        trans, name,
                        should_overwrite    = True,
                        parent_net_key      = parent_net_key,
                        parent_operation_id = parent_operation_id,
                        is_subflow          = is_subflow,
                        peer_net_key        = peer_net_key,
                        peer_operation_id   = peer_operation_id,
                        parallel_index      = parallel_index))

                # update instance
                _perform_insert(conn, insert_instance_dict,
                        table_name="%s.workflow_instance" % self.owner)

                if status is None:
                    status = 'new'

                insert_execution_dict = self._get_update_execution_dict(conn,
                        trans,
                        should_overwrite = True,
                        status           = status,
                        dispatch_id      = dispatch_id,
                        start_time       = start_time,
                        end_time         = end_time,
                        stdout           = stdout,
                        stderr           = stderr,
                        exit_code        = exit_code)
                insert_execution_dict['WORKFLOW_INSTANCE_ID'] = instance_id
                insert_execution_dict['WORKFLOW_EXECUTION_ID'] = execution_id

                _perform_insert(conn, insert_execution_dict,
                        table_name="%s.workflow_instance_execution" % self.owner)

                if created_transaction:
                    trans.commit()
            except:
                trans.rollback()
                raise
        return instance_id

    def _should_overwrite(self, prev_status, new_status):
        if new_status is None:
            return False

        prev_index = STATUSES.index(prev_status)
        try:
            new_index = STATUSES.index(new_status)
        except ValueError:
            raise ValueError("Status must be one of %s, not '%s'" %
                    (STATUSES, new_status))

        return new_index >= prev_index


    def _get_rows(self, conn, trans, instance_id):
        r1 = conn.execute(SELECT_INSTANCE % self.owner,
                workflow_instance_id=instance_id)
        instance_row = r1.fetchone()

        execution_id = instance_row['CURRENT_EXECUTION_ID']
        r2 = conn.execute(SELECT_EXECUTION % self.owner,
                workflow_execution_id = execution_id)
        execution_row = r2.fetchone()
        return instance_row, execution_row

    def _get_update_instance_dict(self, conn, trans, name,
            instance_row        = None,
            should_overwrite    = False,
            parent_net_key      = None,
            parent_operation_id = None,
            is_subflow          = False,
            peer_net_key        = None,
            peer_operation_id   = None,
            parallel_index      = None):

        r = {'NAME': name}

        if parent_net_key is not None:
            if is_subflow:
                r['PARENT_EXECUTION_ID'] = self._get_execution_id(conn,
                        trans, parent_net_key, parent_operation_id)
            else:
                r['PARENT_INSTANCE_ID'] = self._get_instance_id(conn,
                        trans, parent_net_key, parent_operation_id)

        if peer_net_key is not None:
            r['PEER_INSTANCE_ID'] = self._get_instance_id(conn, trans,
                    peer_net_key, peer_operation_id)

        if parallel_index is not None:
            r['PARALLEL_INDEX'] = parallel_index

        return self._generate_update_dict(r, row=instance_row,
                should_overwrite=should_overwrite)

    def _generate_update_dict(self, putative_dict, row=None,
            should_overwrite=False):
        if row is None:
            row = defaultdict(lambda: None)

        update_dict = {}
        for column_name, value in putative_dict.items():
            if row[column_name] is None:
                update_dict[column_name] = value
            else:
                if should_overwrite and row[column_name] != value:
                    update_dict[column_name] = value
        return update_dict


    def _get_update_execution_dict(self, conn, trans,
            execution_row    = None,
            should_overwrite = False,
            status           = None,
            dispatch_id      = None,
            user_name        = None,
            start_time       = None,
            end_time         = None,
            stdout           = None,
            stderr           = None,
            exit_code        = None):

        r = {}
        r['IS_RUNNING'] = status in ['running', 'scheduled']
        r['IS_DONE'] = status == 'done'
        r['STATUS'] = status

        for var_name in ['START_TIME', 'END_TIME', 'STDOUT',
                'STDERR', 'EXIT_CODE']:
            if locals()[var_name.lower()] is not None:
                r[var_name] = locals()[var_name.lower()]

        return self._generate_update_dict(r, row=execution_row,
                should_overwrite=should_overwrite)

    def _get_instance_id(self, conn, trans, net_key, operation_id):
        r = execute_and_log(conn,
                SELECT_INSTANCE_ID % self.owner,
                net_key=net_key,
                operation_id=operation_id)
        rows = r.fetchall()
        if rows:
            instance_id = rows[0][0]
            return instance_id
        return self.update(net_key, operation_id, 'pending',
                conn=conn, trans=trans, recursion_level=self.recursion_level+1)

    def _get_execution_id(self, conn, trans, net_key=None, operation_id=None,
            instance_id=None):
        if instance_id is None:
            instance_id = self._get_instance_id(conn, trans, net_key,
                    operation_id)

        result = execute_and_log(conn,
                SELECT_EXECUTION_ID % self.owner,
                workflow_instance_id=instance_id)
        execution_id = result.fetchone()[0]
        return execution_id


def _perform_insert(engine, idict, table_name):
    if not idict:
        return None
    names = ", ".join(idict.keys())
    place_holders = ", ".join([":%s" % x for x in idict.keys()])
    cmd = "INSERT INTO %s (%s) VALUES (%s)" % (table_name, names, place_holders)

    execute_and_log(engine, cmd, **idict)

def _perform_update(engine, udict, table_name, id_field, update_id):
    if not udict:
        return None
    set_portion = ", ".join(["%s=:%s" % (x,x) for x in udict.keys()])
    cmd = UPDATE % (table_name, set_portion, id_field, id_field)
    udict[id_field] = update_id

    execute_and_log(engine, cmd, **udict)


