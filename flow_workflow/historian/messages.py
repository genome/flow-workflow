from flow.protocol.message import Message
from flow_workflow.historian.status import Status


class UpdateMessage(Message):
    required_fields = {
            'name': basestring,
            'operation_data': dict,
            'status': basestring,
            'user_name': basestring,
            'workflow_plan_id': int,
     }

    optional_fields = {
            'parent_operation_data': dict,
            'is_subflow': bool,

            'peer_operation_data': dict,

            'parallel_index': int,

            'dispatch_id': basestring,

            'start_time': basestring,
            'end_time': basestring,

            'stdout': basestring,
            'stderr': basestring,

            'exit_code': int,
    }

    def validate(self):
        Message.validate(self)
        Status(self.status) # throws ValueError if invalid


class DeleteMessage(Message):
    required_fields = {
        'operation_data': dict,
    }
