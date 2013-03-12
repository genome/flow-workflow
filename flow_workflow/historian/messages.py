from flow.protocol.message import Message

class UpdateMessage(Message):
    required_fields = {
            'net_key': basestring,
            'operation_id': int,
            'name': basestring,
     }

    optional_fields = {
            'xml': basestring,
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
