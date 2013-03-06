from sqlalchemy import create_engine
import logging
from sqlalchemy.exc import IntegrityError

LOG = logging.getLogger(__name__)
EMPTY_FROZEN_HASH = "".join([chr(x) for x in [5,7,3,0,0,0,0]])

class WorkflowHistorianStorage(object):
    def __init__(self, connection_string, owner):
        self.connection_string = connection_string
        self.owner = owner
        self.engine = create_engine(connection_string)

    def _next_id(self, table_name):
        s = "SELECT %s.%s_seq.nextval FROM DUAL" % (self.owner, table_name)
        LOG.debug("EXECUTING: %s", s)
        self.engine.execute(s)
        return self.engine.fetchone()[0]

    def next_instance_id(self):
        return self._next_id('workflow_instance')

    def next_execution_id(self):
        return self._next_id('workflow_execution')

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

        instance_id = self.next_instance_id()
        e = self.engine
        try:
            e.execute("""
                    INSERT INTO %s.workflow_historian (net_key,
                        operation_id, workflow_instance_id) VALUES
                        (:net_key, :operation_id, :workflow_instance_id)""" %
                    self.owner,
                    net_key=net_key,
                    operation_id=operation_id,
                    workflow_instance_id=instance_id)
        except IntegrityError:
            e.execute("""SELECT %s.workflow_instance_id FROM
                    %s.workflow_historian WHERE net_key=:net_key AND
                    operation_id=:operation_id""" % self.owner,
                    net_key=net_key,
                    operation_id=operation_id)
            instance_id = e.fetchone()[0]
            # update instance
            # update instance_execution
        else:
            # insert into instance
            insert = {'workflow_instance_id': instance_id}
            if name is not None:
                insert['name'] = name
            execute_insert(insert,
                    table_name="%s.workflow_instance" % self.owner,
                    engine=e)
            # insert into instance_execution

def execute_insert(idict, table_name=None, engine=None):
    names = ",".join(idict.keys())
    place_holders = ",".join([":%s" % x for x in idict.keys()])
    cmd = "INSERT INTO %s (%s) VALUES (%s)" % (table_name, names, place_holders)

    LOG.debug("EXECUTING: %s", cmd)
    engine.execute(cmd, **idict)


