from flow.petri_net import future
from flow.petri_net import success_failure_net


# XXX Maybe this turns into a historian mixin?
class GenomeNetBase(success_failure_net.SuccessFailureNet):
    def __init__(self, name, operation_id, parent_operation_id=None):
        success_failure_net.SuccessFailureNet.__init__(self, name=name)
        self.operation_id = operation_id
        self.parent_operation_id = parent_operation_id


    def historian_action(self, status, **kwargs):
        info = {"id": self.operation_id,
                "name": self.name,
                "status": status,
                "parent_net_key": None,
                "parent_operation_id": self.parent_operation_id}

        # XXX the name 'parallel_index' is suspicious
        optional_attrs = ['parent_net_key',
                'peer_operation_id', 'parallel_index']
        for attr in optional_attrs:
            value = getattr(self, attr, None)
            if value is not None:
                info[attr] = value

        args = {"children_info": [info]}
        args.update(kwargs)

        return future.FutureAction(cls=WorkflowHistorianUpdateAction, args=args)
