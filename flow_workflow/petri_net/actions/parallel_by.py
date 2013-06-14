from flow.petri_net.actions.base import BasicActionBase, BarrierActionBase
from flow_workflow.io import load
from twisted.internet import defer


class ParallelByJoin(BarrierActionBase):
    def execute(self, net, color_descriptor, active_tokens, service_interfaces):
        workflow_data = load.extract_data_from_tokens(active_tokens)

        # XXX Do we need to store outputs for the whole operation somehow?

        data = {
            'workflow_data': workflow_data,
        }
        token = net.create_token(color=color_descriptor.group.parent_color,
            color_group_idx=color_descriptor.group.parent_color_group_idx,
            data=data)

        return [token], defer.succeed(None)


class ParallelBySplit(BasicActionBase):
    def execute(self, net, color_descriptor, active_tokens, service_interfaces):
        workflow_data = load.extract_data_from_tokens(active_tokens)
        size = workflow_data.pop('parallel_by_size')

        new_color_group = net.add_color_group(size=size,
                parent_color=color_descriptor.color,
                parent_color_group=color_descriptor.group.idx)

        tokens = []
        for parallel_by_idx in xrange(size):
            color = new_color_group.begin + parallel_by_idx
            workflow_data['parallel_by_idx'] = parallel_by_idx

            data = {
                'workflow_data': workflow_data,
            }
            tokens.append(net.create_token(color=color,
                color_group_idx=new_color_group.idx, data=data))

        return tokens, defer.succeed(None)
