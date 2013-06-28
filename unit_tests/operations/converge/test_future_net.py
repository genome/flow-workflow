from unittest import TestCase
from flow_workflow.operations.workflow_net_base import WorkflowNetBase
from flow_workflow.operations.converge.action import ConvergeAction
from flow_workflow.operations.converge.future_net import ConvergeNet

operation_id = object()
input_property_order = object()
input_connections = object()
output_properties = object()
resources = object()
parent_operation_id = object()

net = ConvergeNet(name='test_name',
        operation_id=operation_id,
        input_property_order=input_property_order,
        input_connections=input_connections,
        output_properties=output_properties,
        resources=resources,
        parent_operation_id=parent_operation_id)


class Tests(TestCase):
    def test_init(self):
        self.assertIsInstance(net, WorkflowNetBase)
        self.assertEqual(net.name, 'test_name')

        self.assertIn(net.starting_place,
                net.internal_start_transition.arcs_out)
        self.assertIn(net.converge_transition,
                net.starting_place.arcs_out)
        self.assertIn(net.succeeding_place,
                net.converge_transition.arcs_out)
        self.assertIn(net.internal_success_transition,
                net.succeeding_place.arcs_out)

        expected_args = {
            "operation_id": operation_id,
            "input_property_order": input_property_order,
            "output_properties": output_properties,
            "input_connections": input_connections,
        }

        self.assertItemsEqual(net.converge_transition.action.args,
                expected_args)
        self.assertIs(net.converge_transition.action.cls, ConvergeAction)
