from flow.protocol.message import Message


class NotifyCompletionMessage(Message):
    required_fields = {
            'status': basestring,
    }
