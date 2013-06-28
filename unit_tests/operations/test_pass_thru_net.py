from unittest import TestCase
from flow_workflow.operations.workflow_net_base import WorkflowNetBase
from flow_workflow.operations.pass_thru_net import PassThruNet
from flow_workflow.operations.pass_thru_action import PassThruAction

operation_id = object()
input_connections = object()
output_properties = object()
resources = object()
parent_operation_id = object()

ptn = PassThruNet(operation_id=operation_id,
        input_connections=input_connections,
        output_properties=output_properties,
        resources=resources,
        parent_operation_id=parent_operation_id)

class Tests(TestCase):
    def test_init(self):
        self.assertIsInstance(ptn, WorkflowNetBase)
        self.assertEqual(ptn.name, 'DEFINE IN SUBCLASSES')

        self.assertIn(ptn.starting_place,
                ptn.internal_start_transition.arcs_out)
        self.assertIn(ptn.store_transition,
                ptn.starting_place.arcs_out)
        self.assertIn(ptn.succeeding_place,
                ptn.store_transition.arcs_out)
        self.assertIn(ptn.internal_success_transition,
                ptn.succeeding_place.arcs_out)

        expected_args = {
                'operation_id':ptn._action_arg_operation_id,
                'input_connections':input_connections,
                }

        self.assertItemsEqual(ptn.store_transition.action.args, expected_args)
        self.assertIs(ptn.store_transition.action.cls, PassThruAction)

    def test_private_action_arg_operation_id(self):
        self.assertIs(ptn._action_arg_operation_id, operation_id)
