from flow.protocol.message import Message

class UpdateMessage(Message):
    required_fields = {
            'workflow_plan_id': int,

            'net_key': basestring,
            'operation_id': int,
            'color': int,

            'name': basestring,

            'status': basestring,
     }

    optional_fields = {
            'parent_net_key': basestring,
            'parent_operation_id': int,
            'parent_color': int,

            'peer_net_key': basestring,
            'peer_operation_id': int,
            'peer_color': int,

            'parallel_index': int,

            'dispatch_id': basestring,

            'user_name': basestring,

            'start_time': basestring,
            'end_time': basestring,

            'stdout': basestring,
            'stderr': basestring,

            'exit_code': int,
    }

# XXX use validate or data clumps for net_key/operation_id/color combinations.
