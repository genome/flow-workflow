from sqlalchemy import create_engine
import logging
from sqlalchemy.exc import IntegrityError
from collections import defaultdict
from struct import pack
import re

LOG = logging.getLogger(__name__)

INSERT_INTO_WORKFLOW_HISTORIAN = """
INSERT INTO %s.workflow_historian (net_key, operation_id)
VALUES (:net_key, :operation_id)
"""

UPDATE_WORKFLOW_HISTORIAN = """
UPDATE %s.workflow_historian SET workflow_instance_id = :workflow_instance_id
WHERE net_key=:net_key AND operation_id=:operation_id
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

class WorkflowHistorianStorage(object):
    def __init__(self, connection_string, owner):
        self.connection_string = connection_string
        self.owner = owner
        self.engine = create_engine(connection_string, case_sensitive=False)

    def update(self, net_key, operation_id, name, plan_id, **kwargs):
        transaction = kwargs.pop('transaction', None)
        recursion_level = kwargs.pop('recursion_level', 0)

        hdict = kwargs
        #   Plan_id should be set when you first create the instance/execution
        # rows, however, because we may have to auto-generate parents (to
        # satisfy foreign key constraints) we might have made a parent with the
        # wrong plan_id.  This will ensure that when the instance with the wrong
        # plan id is updated, it will get the correct one put in.
        hdict['workflow_plan_id'] = plan_id

        if recursion_level > 1:
            raise RuntimeError("update should never recurse more than once!");

        status = kwargs.get('status', None)
        if status is not None:
            try:
                STATUSES.index(status)
            except ValueError:
                raise ValueError("Status must be one of %s, not '%s'" %
                        (STATUSES, status))

        if transaction is None:
            transaction = SimpleTransaction(self.engine)

        try:
            execute_and_log(transaction,
                    INSERT_INTO_WORKFLOW_HISTORIAN % self.owner,
                    net_key=net_key,
                    operation_id=operation_id)
        except IntegrityError:
            LOG.debug("Failed to insert (net_key=%s, operation_id=%s) into "
                    "workflow_historian table, attempting update instead." %
                    (net_key, operation_id))
            try:
                instance_id = self._get_instance_id(transaction,
                        recursion_level, net_key, operation_id, plan_id)
                instance_row, execution_row = self._get_rows(transaction,
                        instance_id)

                execution_id = instance_row['CURRENT_EXECUTION_ID']

                should_overwrite = self._should_overwrite(
                        execution_row['STATUS'], status)

                update_instance_dict = self._get_update_instance_dict(
                        transaction      = transaction,
                        recursion_level  = recursion_level,
                        hdict            = hdict,
                        plan_id          = plan_id,
                        instance_row     = instance_row,
                        should_overwrite = should_overwrite)

                self._update_instance(transaction, update_instance_dict,
                        instance_id)

                update_execution_dict = self._get_update_execution_dict(
                        transaction      = transaction,
                        hdict            = hdict,
                        execution_row    = execution_row,
                        should_overwrite = should_overwrite)

                self._update_execution(transaction, update_execution_dict,
                        execution_id)

                if recursion_level == 0:
                    transaction.commit()
            except:
                transaction.rollback()
                raise
        except:
            transaction.rollback()
            raise
        else:
            try:
                # update workflow_historian table
                instance_id = self.next_instance_id(transaction)
                execute_and_log(transaction,
                        UPDATE_WORKFLOW_HISTORIAN % self.owner,
                        net_key=net_key,
                        operation_id=operation_id,
                        workflow_instance_id=instance_id)

                # insert into instance
                execution_id = self.next_execution_id(transaction)
                insert_instance_dict = {
                        'NAME': name,
                        'WORKFLOW_INSTANCE_ID': instance_id,
                }

                insert_instance_dict.update(self._get_update_instance_dict(
                        transaction      = transaction,
                        recursion_level  = recursion_level,
                        hdict            = hdict,
                        plan_id          = plan_id,
                        instance_row     = None,
                        should_overwrite = True))

                _perform_insert(transaction, insert_instance_dict,
                        table_name="%s.workflow_instance" % self.owner)

                # insert into execution
                if hdict.get('status', None) is None:
                    hdict['status'] = 'new'

                insert_execution_dict = self._get_update_execution_dict(
                        transaction      = transaction,
                        hdict            = hdict,
                        execution_row    = None,
                        should_overwrite = True)
                insert_execution_dict['WORKFLOW_EXECUTION_ID'] = execution_id
                insert_execution_dict['WORKFLOW_INSTANCE_ID'] = instance_id

                _perform_insert(transaction, insert_execution_dict,
                        table_name="%s.workflow_instance_execution" %
                                self.owner)

                self._update_instance(transaction,
                        {'CURRENT_EXECUTION_ID': execution_id},
                        instance_id)

                if recursion_level == 0:
                    transaction.commit()
            except:
                transaction.rollback()
                raise
        return instance_id

    def _update_instance(self, transaction, update_dict, instance_id):
        return _perform_update(transaction, update_dict,
                table_name = "%s.workflow_instance" % self.owner,
                id_field   = "workflow_instance_id",
                update_id  = instance_id)

    def _update_execution(self, transaction, update_dict, execution_id):
        return _perform_update(transaction, update_dict,
                table_name="%s.workflow_instance_execution" % self.owner,
                id_field="workflow_execution_id",
                update_id=execution_id)

    def _next_id(self, transaction, table_name):
        stmnt = "SELECT %s.%s_seq.nextval FROM DUAL" % (self.owner, table_name)
        result = execute_and_log(transaction, stmnt)
        return result.fetchone()[0]

    def next_instance_id(self, transaction):
        return self._next_id(transaction, 'workflow_instance')

    def next_execution_id(self, transaction):
        return self._next_id(transaction, 'workflow_execution')

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

    def _get_rows(self, transaction, instance_id):
        """
        Return the row in the WORKFLOW_INSTANCE table and the
        WORKFLOW_INSTANCE_EXECUTION table
        """
        r1 = transaction.execute(SELECT_INSTANCE % self.owner,
                workflow_instance_id=instance_id)
        instance_row = r1.fetchone()

        execution_id = instance_row['CURRENT_EXECUTION_ID']

        r2 = transaction.execute(SELECT_EXECUTION % self.owner,
                workflow_execution_id = execution_id)
        execution_row = r2.fetchone()
        return instance_row, execution_row

    def _get_update_instance_dict(self, transaction, recursion_level, hdict, plan_id,
            instance_row, should_overwrite):

        putative_dict = {}

        parent_net_key = hdict.get('parent_net_key', None)
        parent_operation_id = hdict.get('parent_operation_id', None)
        if parent_net_key is not None:
            if hdict.get('is_subflow', None):
                putative_dict['PARENT_EXECUTION_ID'] = self._get_execution_id(
                        transaction, recursion_level,
                        net_key      = parent_net_key,
                        operation_id = parent_operation_id,
                        plan_id      = plan_id)
            else:
                putative_dict['PARENT_INSTANCE_ID'] = self._get_instance_id(
                        transaction, recursion_level, parent_net_key,
                        parent_operation_id, plan_id)

        peer_net_key = hdict.get('peer_net_key', None)
        peer_operation_id = hdict.get('peer_operation_id', None)
        if peer_net_key is not None:
            putative_dict['PEER_INSTANCE_ID'] = self._get_instance_id(
                    transaction, recursion_level, peer_net_key,
                    peer_operation_id, plan_id)

        for var_name in ['PARALLEL_INDEX', 'WORKFLOW_PLAN_ID']:
            if hdict.get(var_name.lower(), None) is not None:
                putative_dict[var_name] = hdict[var_name.lower()]

        return self._generate_update_dict(putative_dict, row=instance_row,
                should_overwrite=should_overwrite)

    def _get_update_execution_dict(self, transaction, hdict, execution_row,
            should_overwrite):
        putative_dict = {}
        status = hdict.get('status', None)
        putative_dict['IS_RUNNING'] = status in ['running', 'scheduled']
        putative_dict['IS_DONE'] = status == 'done'
        putative_dict['STATUS'] = status

        for var_name in ['DISPATCH_ID', 'START_TIME', 'END_TIME', 'STDOUT',
                'STDERR', 'EXIT_CODE']:
            if hdict.get(var_name.lower(), None) is not None:
                putative_dict[var_name] = hdict[var_name.lower()]

        return self._generate_update_dict(putative_dict, row=execution_row,
                should_overwrite=should_overwrite)

    def _generate_update_dict(self, putative_dict,
            row              = None,
            should_overwrite = False):
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

    def _get_instance_id(self, transaction, recursion_level, net_key,
            operation_id, plan_id):
        result = execute_and_log(transaction, SELECT_INSTANCE_ID % self.owner,
                net_key=net_key, operation_id=operation_id)
        rows = result.fetchall()
        if rows:
            instance_id = rows[0][0]
            if instance_id is not None:
                return instance_id
        return self.update(net_key, operation_id, 'pending', plan_id,
                transaction=transaction, recursion_level=recursion_level+1)

    def _get_execution_id(self, transaction, recursion_level, plan_id,
            net_key      = None,
            operation_id = None,
            instance_id  = None):
        if instance_id is None:
            instance_id = self._get_instance_id(transaction,
                    recursion_level = recursion_level,
                    net_key         = net_key,
                    operation_id    = operation_id,
                    plan_id         = plan_id)

        result = execute_and_log(transaction, SELECT_EXECUTION_ID % self.owner,
                workflow_instance_id=instance_id)
        execution_id = result.fetchone()[0]
        return execution_id


class SimpleTransaction(object):
    def __init__(self, engine):
        self.engine = engine
        self.begin_transaction()

    def begin_transaction(self):
        self.conn = self.engine.connect()
        self.trans = self.conn.begin()

    def execute(self, *args, **kwargs):
        return self.conn.execute(*args, **kwargs)

    def commit(self, *args, **kwargs):
        rv = self.trans.commit(*args, **kwargs)
        self.conn.close()
        return rv

    def rollback(self, *args, **kwargs):
        rv = self.trans.rollback(*args, **kwargs)
        self.conn.close()
        return rv


def execute_and_log(transaction, stmnt, **kwargs):
    full_stmnt = stmnt
    for name, value in kwargs.items():
        full_stmnt = re.sub(r':' + name, str(value), full_stmnt)
    LOG.debug("EXECUTING: %s", full_stmnt)
    return transaction.execute(stmnt, **kwargs)


def _perform_insert(transaction, idict, table_name):
    if not idict:
        return None
    names = ", ".join(idict.keys())
    place_holders = ", ".join([":%s" % x for x in idict.keys()])
    cmd = "INSERT INTO %s (%s) VALUES (%s)" % (table_name, names, place_holders)

    execute_and_log(transaction, cmd, **idict)


def _perform_update(transaction, udict, table_name, id_field, update_id):
    if not udict:
        return None
    set_portion = ", ".join(["%s=:%s" % (x,x) for x in udict.keys()])
    cmd = UPDATE % (table_name, set_portion, id_field, id_field)
    udict[id_field] = update_id

    execute_and_log(transaction, cmd, **udict)


