from flow.petri_net import future
from flow_workflow import io
from flow_workflow.operations import factory


class Workflow(object):
    def __init__(self, xml, inputs, resources):
        self.xml = xml
        self.inputs = inputs
        self.resources = resources

        self.dummy_operation = factory.operation('null')

    def store_inputs(self, net):
        io.store_outputs(net, self.dummy_operation.operation_id, self.inputs)

    @property
    def input_connections(self):
        return {
            self.dummy_operation.operation_id:
                {name: name for name, value in self.inputs.iteritems()}
        }

    @property
    def output_properties(self):
        return self.operation.output_properties

    @property
    def operation(self):
        return factory.operation_from_xml(self.xml,
                parent=factory.operation('null'))

    @property
    def operation_future_net(self):
        return self.operation.net(self.input_connections,
                self.output_properties, self.resources)

    @property
    def future_net(self):
        return WorkflowNet(self.operation_future_net)


class WorkflowNet(future.FutureNet):
    def __init__(self, operation_net):
        future.FutureNet.__init__(self)

        self.start_place = self.add_place('start')
        self.operation_net = operation_net

        self.start_place.add_arc_out(operation_net.start_transition)
