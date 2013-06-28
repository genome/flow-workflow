from unittest import TestCase
from flow_workflow.operations.connectors.future_nets import (
        InputConnectorNet, OutputConnectorNet)
from flow_workflow.operations.pass_thru_net import PassThruNet


operation_id = object()
input_connections = object()
output_properties = object()
resources = object()
parent_operation_id = object()

class Tests(TestCase):
    def test_input_connector_net(self):
        net = InputConnectorNet(operation_id=operation_id,
                input_connections=input_connections,
                output_properties=output_properties,
                resources=resources,
                parent_operation_id=parent_operation_id)

        self.assertIsInstance(net, PassThruNet)
        self.assertEqual(net.name, 'input-connector')

    def test_output_connector_net(self):
        net = OutputConnectorNet(operation_id=operation_id,
                input_connections=input_connections,
                output_properties=output_properties,
                resources=resources,
                parent_operation_id=parent_operation_id)

        self.assertIsInstance(net, PassThruNet)
        self.assertEqual(net.name, 'output-connector')
        self.assertIs(net._action_arg_operation_id, parent_operation_id)
