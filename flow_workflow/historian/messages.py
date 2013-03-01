from flow.protocol.message import Message

class CreateOperationMessage(Message):
    required_fields = {
            'operation_key': basestring,
            'name': basestring,
     }

    optional_fields = {
            'parent_key': basestring,
            'peer_key': basestring,
            'parallel_index': int,
            'xml': basestring,
            'is_subflow': bool,
    }


class UpdateOperationMessage(Message):
    required_fields = {
            'operation_key': basestring,
            'status': basestring,
     }

    optional_fields = {
            'start_time': basestring,
            'end_time': basestring,
            'stdout': basestring,
            'stderr': basestring,
            'exit_code': int,
    }
