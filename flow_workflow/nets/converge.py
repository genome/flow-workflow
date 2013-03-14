from flow_workflow.nets.core import InputsMixin, GenomeEmptyNet
from flow_workflow.nets.io import *

import flow.petri.safenet as sn
import flow.petri.netbuilder as nb


def _do_converge(inputs, input_property_order, output_properties):
    out_list = [inputs[x] for x in input_property_order]
    return {prop: out_list for prop in output_properties}


class GenomeConvergeAction(InputsMixin, sn.TransitionAction):
    required_arguments = ["operation_id", "input_property_order",
            "output_properties"]

    output_token_type = "output"

    def execute(self, active_tokens_key, net, service_interfaces):
        operation_id = self.args["operation_id"]
        input_property_order = self.args["input_property_order"]
        output_properties = self.args["output_properties"]

        input_data = self.input_data(active_tokens_key, net)

        outputs = _do_converge(input_data, input_property_order,
                output_properties)

        store_outputs(outputs, net, operation_id)


class GenomeConvergeNet(GenomeEmptyNet):
    def __init__(self, builder, name, operation_id, parent_operation_id,
            input_connections, input_property_order, output_properties,
            stdout=None, stderr=None):

        GenomeEmptyNet.__init__(self, builder, name, operation_id,
                parent_operation_id, input_connections, queue=None, resources=None)

        self.input_property_order = input_property_order
        self.output_properties = output_properties

        args = {
            "operation_id": self.operation_id,
            "with_outputs": True,
            "input_property_order": self.input_property_order,
            "output_properties": self.output_properties,
            "input_connections": self.input_connections,
            "stdout": stdout,
            "stderr": stderr,
        }

        action = nb.ActionSpec(cls=GenomeConvergeAction, args=args)

        self.start_transition = self.add_transition("converge",
                action=action)

        self.success_transition = self.add_transition("update historian",
                action=self._update_action("done"))

        self.bridge_transitions(self.start_transition, self.success_transition)
