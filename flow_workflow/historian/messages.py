from flow.protocol.message import Message

class UpdateMessage(Message):
    required_fields = {
            'net_key': basestring,
            'operation_id': int,
            'name': basestring,
            'workflow_plan_id': int,
     }

    optional_fields = {
            'parent_net_key': basestring,
            'parent_operation_id': int,
            'peer_net_key': basestring,
            'peer_operation_id': int,
            'parallel_index': int,
            'is_subflow': bool,
            'status': basestring,
            'dispatch_id': basestring,
            'user_name': basestring,
            'start_time': basestring,
            'end_time': basestring,
            'stdout': basestring,
            'stderr': basestring,
            'exit_code': int,
    }
