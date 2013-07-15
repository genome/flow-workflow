from flow_workflow.entities.future_nets import WorkflowNetBase


class ModelNet(WorkflowNetBase):
    def __init__(self, name, operation_id, input_connections,
            subnets, edges, parent_operation_id=None):
        WorkflowNetBase.__init__(self, name=name,
                operation_id=operation_id,
                input_connections=input_connections,
                resources=None,
                parent_operation_id=parent_operation_id)

        for name, subnet in subnets.iteritems():
            self.subnets.add(subnet)

        for source_name, dest_names in edges.iteritems():
            for dest_name in dest_names:
                self.bridge_transitions(subnets[source_name].success_transition,
                        subnets[dest_name].start_transition)

        self.starting_place = self.bridge_transitions(
                self.internal_start_transition,
                subnets['input connector'].start_transition,
                name='starting')

        self.succeeding_place = self.bridge_transitions(
                subnets['output connector'].success_transition,
                self.internal_success_transition,
                name='succeeding')

        self.failing_place = self.join_transitions_as_or(
                self.internal_failure_transition,
                [s.failure_transition for s in subnets.itervalues()],
                name='failing')
