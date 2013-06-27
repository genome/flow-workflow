from flow.petri_net.success_failure_net import SuccessFailureNet

class Workflow(object):
    def __init__(self, adapter_factory, xml_etree, resources, plan_id):
        self.adapter_factory = adapter_factory
        self.plan_id = plan_id

        # We have to force the outer xml to be a model.
        # Sometimes, a workflow consists of a single <operation> tag instead of
        # a <workflow> tag
        self.model = self.adapter_factory.create("Workflow::OperationType::Model",
                xml_etree, log_dir=None, resources=resources)

        self.outer_net = SuccessFailureNet(name=self.model.name)
        self.outer_net.wrap_in_places()

        self.inner_net = self.model.net(super_net=self.outer_net)

        outer_net.starting_place = outer_net.bridge_transitions(
                outer_net.internal_start_transition,
                inner_net.start_transition,
                name='starting')
        outer_net.succeeding_place = outer_net.bridge_transitions(
                inner_net.success_transition,
                outer_net.internal_success_transition,
                name='succeeding')
        outer_net.failing_place = outer_net.bridge_transitions(
                inner_net.failure_transition,
                outer_net.internal_failure_transition,
                name='failing')

    def get_variables_and_constants(self):
        variables = {}
        next_id = self.adapter_factory.next_operation_id
        variables['workflow_next_operation_id'] = next_id

        constants = {}
        constants['workflow_id'] = self.model.operation_id
        constants['workflow_plan_id'] = self.plan_id

        parent = os.environ.get('FLOW_WORKFLOW_PARENT_ID')
        if parent:
            LOG.info('Setting parent workflow to %s' % parent)
            parent_net_key, parent_op_id = parent.split(' ')
            constants['workflow_parent_net_key'] = parent_net_key
            constants['workflow_parent_operation_id'] = int(
                    parent_op_id)

        return variables, constants
