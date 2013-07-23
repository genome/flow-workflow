import json

class OperationData(object):
    def __init__(self, net_key, operation_id, color):
        self.net_key = net_key
        self.operation_id = int(operation_id)
        self.color = int(color)

    def dumps(self):
        return json.dumps(self.to_dict, sort_keys=True)

    @classmethod
    def loads(cls, string):
        return cls.from_dict(json.loads(string))

    @property
    def to_dict(self):
        return {
            'net_key': self.net_key,
            'operation_id': self.operation_id,
            'color': self.color
        }

    @classmethod
    def from_dict(cls, operation_data_dict):
        return cls(**operation_data_dict)

    def __repr__(self):
        return str(self)

    def __str__(self):
        return "OperationData(net_key='%s', operation_id=%s, color=%s)" % (
                self.net_key, self.operation_id, self.color)

    def __eq__(self, other):
        return self.to_dict == other.to_dict
