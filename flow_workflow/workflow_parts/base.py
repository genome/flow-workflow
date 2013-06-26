
class WorkflowPart(object):
    def __init__(self, part_factory, operation_id, parent=None):
        self.part_factory = part_factory
        self.operation_id = operation_id
        self.parent = parent
        self.parent_id = self.parent.operation_id if parent else None
        self.notify_historian = True

    def net(self, super_net, input_connections=None):
        raise NotImplementedError("net not implemented in %s" %
                                  self.__class__.__name__)

    @property
    def children(self):
        return []

class Workflow(object):
    def __init__(self, part_factory, xml_etree, resources, plan_id):
        self.part_factory = part_factory
        self.plan_id = plan_id

        # We have to force the outer xml to be a model.
        # Sometimes, a workflow consists of a single <operation> tag instead of
        # a <workflow> tag
        self.model = self.part_factory.create("Workflow::OperationType::Model",
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

        #outer_net.bridge_places(inner_net.start_place,
        #        outer_net.internal_start_place)
        #outer_net.bridge_places(inner_net.success_place,
        #        outer_net.internal_success_place)
        #outer_net.bridge_places(inner_net.failure_place,
        #        outer_net.internal_failure_place)

    def get_variables_and_constants(self):
        variables = {}
        next_id = self.part_factory.next_operation_id
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


class WorkflowOperation(WorkflowPart):
    # adds xml and log_dir
    def __init__(self, part_factory, operation_id, xml, log_dir, parent):
        self.xml = xml
        self.log_dir = log_dir

        WorkflowPart.__init__(self, operation_id=operation_id, parent=parent)

        type_nodes = xml.findall("operationtype")
        self.name = xml.attrib["name"]
        if len(type_nodes) != 1:
            raise ValueError(
                "Wrong number of <operationtype> tags in operation %s" %
                self.name
            )

        self._type_node = type_nodes[0]
        self._operation_attributes = xml.attrib
        self._type_attributes = self._type_node.attrib

        basename = log_file_name(self.name)
        out_file = "%s.%d.out" % (basename, operation_id)
        err_file = "%s.%d.err" % (basename, operation_id)
        self.stdout_log_file = os.path.join(log_dir, out_file)
        self.stderr_log_file = os.path.join(log_dir, err_file)

