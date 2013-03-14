from sqlalchemy import create_engine
import logging
from sqlalchemy.exc import IntegrityError
from collections import defaultdict, namedtuple
from struct import pack
import re

LOG = logging.getLogger(__name__)

STATEMENTS = {}
STATEMENTS['INSERT_INTO_WORKFLOW_HISTORIAN'] = """
INSERT INTO %s.workflow_historian (net_key, operation_id)
VALUES (:net_key, :operation_id)
"""

STATEMENTS['UPDATE_WORKFLOW_HISTORIAN'] = """
UPDATE %s.workflow_historian SET workflow_instance_id = :workflow_instance_id
WHERE net_key=:net_key AND operation_id=:operation_id
"""

STATEMENTS['SELECT_INSTANCE_ID'] = """
SELECT workflow_instance_id FROM %s.workflow_historian WHERE
net_key=:net_key AND operation_id=:operation_id
"""

STATEMENTS['SELECT_EXECUTION_ID'] = """
SELECT current_execution_id FROM %s.workflow_instance WHERE
workflow_instance_id=:workflow_instance_id
"""

STATEMENTS['SELECT_INSTANCE'] = """
SELECT * from %s.workflow_instance
WHERE workflow_instance_id = :workflow_instance_id
"""

STATEMENTS['SELECT_EXECUTION'] = """
SELECT * from %s.workflow_instance_execution
WHERE workflow_execution_id = :workflow_execution_id
"""

STATUSES = [
        'new',
        'scheduled',
        'running',
        'failed',
        'crashed',
        'done',
]

Tables = namedtuple('Tables', ['historian', 'instance', 'execution'])
Sequences = namedtuple('Sequences', ['instance', 'execution'])

class WorkflowHistorianStorage(object):
    def __init__(self, connection_string, owner):
        for statement_name, statement in STATEMENTS.items():
            setattr(self, statement_name, statement % owner)

        self.tables = Tables(historian='%s.workflow_historian' % owner,
                    instance='%s.workflow_instance' % owner,
                    execution='%s.workflow_instance_execution' % owner)
        self.sequences = Sequences(instance='%s.workflow_instance_seq' % owner,
                execution='%s.workflow_execution_seq' % owner)

        self.connection_string = connection_string
        self.engine = create_engine(connection_string, case_sensitive=False)

    def update(self, update_info, transaction=None, recursion_level=0):
        if recursion_level > 1:
            raise RuntimeError("update should never recurse more than once!");

        status_info = validate_update_info(update_info)

        if transaction is None:
            transaction = SimpleTransaction(self.engine)

        try:
            execute_and_log(transaction,
                    self.INSERT_INTO_WORKFLOW_HISTORIAN,
                    net_key      = update_info['net_key'],
                    operation_id = update_info['operation_id'])
        except IntegrityError:
            LOG.debug("Failed to insert (net_key=%s, operation_id=%s) into "
                    "workflow_historian table, attempting update instead." %
                    (update_info['net_key'], update_info['operation_id']))
            try:
                instance_id = self._get_instance_id(transaction,
                        recursion_level  = recursion_level,
                        net_key          = update_info['net_key'],
                        operation_id     = update_info['operation_id'],
                        workflow_plan_id = update_info['workflow_plan_id'])
                instance_row, execution_row = self._get_rows(transaction,
                        instance_id)

                execution_id = instance_row['CURRENT_EXECUTION_ID']

                should_overwrite = self._should_overwrite(
                        execution_row['STATUS'], update_info['status'])

                update_instance_dict = self._get_update_instance_dict(
                        transaction      = transaction,
                        recursion_level  = recursion_level,
                        update_info      = update_info,
                        instance_row     = instance_row,
                        should_overwrite = should_overwrite)

                self._update_instance(transaction, update_instance_dict,
                        instance_id)

                update_execution_dict = self._get_update_execution_dict(
                        transaction      = transaction,
                        update_info      = update_info,
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
                        self.UPDATE_WORKFLOW_HISTORIAN,
                        net_key              = update_info['net_key'],
                        operation_id         = update_info['operation_id'],
                        workflow_instance_id = instance_id)

                # insert into instance
                execution_id = self.next_execution_id(transaction)
                insert_instance_dict = {
                        'WORKFLOW_INSTANCE_ID': instance_id,
                }

                insert_instance_dict.update(self._get_update_instance_dict(
                        transaction      = transaction,
                        recursion_level  = recursion_level,
                        update_info      = update_info,
                        instance_row     = None,
                        should_overwrite = True))

                _perform_insert(transaction, insert_instance_dict,
                        table_name=self.tables.instance)

                insert_execution_dict = self._get_update_execution_dict(
                        transaction      = transaction,
                        update_info      = update_info,
                        execution_row    = None,
                        should_overwrite = True)
                insert_execution_dict['WORKFLOW_EXECUTION_ID'] = execution_id
                insert_execution_dict['WORKFLOW_INSTANCE_ID'] = instance_id

                _perform_insert(transaction, insert_execution_dict,
                        table_name=self.tables.execution)

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
                table_name = self.tables.instance,
                id_field   = "workflow_instance_id",
                update_id  = instance_id)

    def _update_execution(self, transaction, update_dict, execution_id):
        return _perform_update(transaction, update_dict,
                table_name=self.tables.execution,
                id_field="workflow_execution_id",
                update_id=execution_id)

    def _next_id(self, transaction, sequence_name):
        NEXT_ID = "SELECT %s.nextval FROM DUAL"
        stmnt = NEXT_ID % sequence_name
        result = execute_and_log(transaction, stmnt)
        return result.fetchone()[0]

    def next_instance_id(self, transaction):
        return self._next_id(transaction, self.sequences.instance)

    def next_execution_id(self, transaction):
        return self._next_id(transaction, self.sequences.execution)

    def _should_overwrite(self, prev_status, new_status):
        if new_status is None:
            return False

        prev_index = STATUSES.index(prev_status)
        new_index = STATUSES.index(new_status)

        return new_index >= prev_index

    def _get_rows(self, transaction, instance_id):
        """
        Return the row in the WORKFLOW_INSTANCE table and the
        WORKFLOW_INSTANCE_EXECUTION table
        """
        r1 = transaction.execute(self.SELECT_INSTANCE,
                workflow_instance_id=instance_id)
        instance_row = r1.fetchone()

        execution_id = instance_row['CURRENT_EXECUTION_ID']

        r2 = transaction.execute(self.SELECT_EXECUTION,
                workflow_execution_id = execution_id)
        execution_row = r2.fetchone()
        return instance_row, execution_row

    def _get_update_instance_dict(self, transaction, recursion_level,
            update_info, instance_row, should_overwrite):
        putative_dict = {}

        parent_net_key = update_info.get('parent_net_key', None)
        parent_operation_id = update_info.get('parent_operation_id', None)
        if parent_net_key is not None:
            if update_info.get('is_subflow', None):
                putative_dict['PARENT_EXECUTION_ID'] = self._get_execution_id(
                        transaction, recursion_level,
                        net_key          = update_info['parent_net_key'],
                        operation_id     = update_info['parent_operation_id'],
                        workflow_plan_id = update_info['workflow_plan_id'])
            else:
                putative_dict['PARENT_INSTANCE_ID'] = self._get_instance_id(
                        transaction, recursion_level,
                        net_key          = update_info['parent_net_key'],
                        operation_id     = update_info['parent_operation_id'],
                        workflow_plan_id = update_info['workflow_plan_id'])

        peer_net_key = update_info.get('peer_net_key', None)
        peer_operation_id = update_info.get('peer_operation_id', None)
        if peer_net_key is not None:
            putative_dict['PEER_INSTANCE_ID'] = self._get_instance_id(
                    transaction, recursion_level,
                    net_key          = update_info['peer_net_key'],
                    operation_id     = update_info['peer_operation_id'],
                    workflow_plan_id = update_info['workflow_plan_id'])

        for var_name in ['PARALLEL_INDEX', 'WORKFLOW_PLAN_ID', 'NAME']:
            if update_info.get(var_name.lower(), None) is not None:
                putative_dict[var_name] = update_info[var_name.lower()]

        return self._generate_update_dict(putative_dict, row=instance_row,
                should_overwrite=should_overwrite)

    def _get_update_execution_dict(self, transaction, update_info,
            execution_row, should_overwrite):
        putative_dict = {}

        status = update_info.get('status', None)
        putative_dict['IS_RUNNING'] = status in ['running', 'scheduled']
        putative_dict['IS_DONE'] = status == 'done'
        putative_dict['STATUS'] = status

        for var_name in ['DISPATCH_ID', 'START_TIME', 'END_TIME', 'STDOUT',
                'STDERR', 'EXIT_CODE']:
            if update_info.get(var_name.lower(), None) is not None:
                putative_dict[var_name] = update_info[var_name.lower()]

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
            operation_id, workflow_plan_id):
        result = execute_and_log(transaction, self.SELECT_INSTANCE_ID,
                net_key=net_key, operation_id=operation_id)
        rows = result.fetchall()
        if rows:
            instance_id = rows[0][0]
            if instance_id is not None:
                return instance_id
        update_info = {
                'net_key':net_key,
                'operation_id':operation_id,
                'name':'pending',
                'workflow_plan_id': workflow_plan_id,
        }
        return self.update(update_info, transaction=transaction,
                recursion_level=recursion_level+1)

    def _get_execution_id(self, transaction, recursion_level, workflow_plan_id,
            net_key, operation_id):
        instance_id = self._get_instance_id(transaction,
                recursion_level  = recursion_level,
                net_key          = net_key,
                operation_id     = operation_id,
                workflow_plan_id = workflow_plan_id)

        result = execute_and_log(transaction, self.SELECT_EXECUTION_ID,
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


def validate_update_info(update_info):
    validated_update_info = update_info

    status = update_info.get('status', None)
    if status is None:
        validated_update_info['status'] = 'new'
    elif status not in STATUSES:
        raise ValueError("Status must be one of %s, not '%s'" %
                (STATUSES, status))
    return validate_update_info


def execute_and_log(transaction, statement, **kwargs):
    log_statement(statement, **kwargs)
    return transaction.execute(statement, **kwargs)


def log_statement(statement, **kwargs):
    debug_statement = statement
    for name, value in kwargs.items():
        debug_statement = re.sub(r':' + name, str(value), debug_statement)
    debug_statement = re.sub('\n', ' ', debug_statement)
    LOG.debug("EXECUTING: %s", debug_statement)


def _perform_insert(transaction, idict, table_name):
    INSERT = "INSERT INTO %s (%s) VALUES (%s)"

    names = ", ".join(idict.keys())
    place_holders = ", ".join([":%s" % x for x in idict.keys()])
    cmd = INSERT % (table_name, names, place_holders)

    execute_and_log(transaction, cmd, **idict)


def _perform_update(transaction, udict, table_name, id_field, update_id):
    if not udict:
        # nothing to update
        return None
    UPDATE = "UPDATE %s SET %s WHERE %s=:%s"

    set_portion = ", ".join(["%s=:%s" % (x,x) for x in udict.keys()])
    cmd = UPDATE % (table_name, set_portion, id_field, id_field)
    udict[id_field] = update_id

    execute_and_log(transaction, cmd, **udict)


