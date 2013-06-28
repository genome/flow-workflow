from unittest import TestCase
from flow_workflow.operations.block.future_net import BlockNet
from flow_workflow.operations.pass_thru_net import PassThruNet

operation_id = object()
input_connections = object()
output_properties = object()
resources = object()
parent_operation_id = object()

class Tests(TestCase):
    def test_init(self):
        net = BlockNet(operation_id=operation_id,
                input_connections=input_connections,
                output_properties=output_properties,
                resources=resources,
                parent_operation_id=parent_operation_id)

        self.assertIsInstance(net, PassThruNet)
        self.assertEqual(net.name, 'block')
