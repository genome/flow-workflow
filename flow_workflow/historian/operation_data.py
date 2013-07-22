class OperationData(object):
    def __init__(self, net_key, operation_id, color):
        self.net_key = net_key
        self.operation_id = int(operation_id)
        self.color = int(color)

    def instance_id(self, historian_storage_thinggy):
        pass

    def execution_id(self, historian_storage_thinggy):
        pass

    @property
    def as_dict(self):
        return {
            'net_key': self.net_key,
            'operation_id': self.operation_id,
            'color': self.color
        }
