from flow.protocol.message import Message
from flow_workflow.historian.status import Status

class UpdateMessage(Message):
    required_fields = {
            'workflow_plan_id': int,

            'operation_data': dict,

            'name': basestring,

            'status': basestring,
     }

    optional_fields = {
            'parent_operation_data': dict,

            'peer_operation_data': dict,

            'parallel_index': int,

            'dispatch_id': basestring,

            'user_name': basestring,

            'start_time': basestring,
            'end_time': basestring,

            'stdout': basestring,
            'stderr': basestring,

            'exit_code': int,
    }

    def validate(self):
        Message.validate(self)
        Status(self.status) # throws ValueError if invalid
# XXX use data clumps for net_key/operation_id/color combinations.
