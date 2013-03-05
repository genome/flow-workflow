from flow.protocol.message import Message

class UpdateMessage(Message):
    required_fields = {
            'net_key': basestring,
            'operation_id': int,
     }

    optional_fields = {
            'name': basestring,
            'status': basestring,
            'parent_key': basestring,
            'peer_key': basestring,
            'parallel_index': int,
            'xml': basestring,
            'is_subflow': bool,
            'start_time': basestring,
            'end_time': basestring,
            'stdout': basestring,
            'stderr': basestring,
            'exit_code': int,
    }
