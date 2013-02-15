import flow.command_runner.executors.nets as enets
import flow.redisom as rom
import flow.petri.netbuilder as nb
import flow.petri.safenet as sn

import logging

LOG = logging.getLogger(__name__)

GENOME_WRAPPER = "workflow-wrapper"

def _output_variable_name(operation_id, output_name):
    return "_wf_outp_%d_%s" % (operation_id, output_name)


def _store_outputs(outputs, net, operation_id):
    for k, v in outputs.iteritems():
        name = _output_variable_name(operation_id, k)
        net.set_variable(name, v)


def _do_converge(inputs, input_property_order, output_properties):
    out_list = [inputs[x] for x in input_property_order]
    return {prop: out_list for prop in output_properties}


def _flatten_input_connections(input_connections):
    if input_connections is None:
        return {}

    flat = {}
    for src, props in input_connections.iteritems():
        for dst_prop, src_prop in props.iteritems():
            src_name = _output_variable_name(src, src_prop)
            flat[src_name] = dst_prop
    return flat


class InputsMixin(object):
    def input_data(self, active_tokens_key, net):
        inputs = {}
        data_arcs = self.args["input_connections"]
        if data_arcs:
            for src_name, dst_name in data_arcs.iteritems():
                value = net.variable(src_name)
                if value:
                    inputs[dst_name] = value

        return inputs


class GenomeShortcutAction(InputsMixin, enets.LocalDispatchAction):
    output_token_type = "input"

    def _command_line(self, net, input_data_key):
        return [GENOME_WRAPPER, self.args["action_type"], "shortcut",
                self.args["action_id"]]


class GenomeExecuteAction(InputsMixin, enets.LSFDispatchAction):
    output_token_type = "input"

    action_type = rom.Property(rom.String)

    def _command_line(self, net, input_data_key):
        return [GENOME_WRAPPER, self.args["action_type"], "execute",
                self.args["action_id"]]


class GenomeConvergeAction(InputsMixin, sn.TransitionAction):
    output_token_type = "output"

    def execute(self, input_data, net, services):
        operation_id = self.args["operation_id"]
        input_property_order = self.args["input_property_order"]
        output_properties = self.args["output_properties"]

        outputs = _do_converge(input_data, input_property_order,
                output_properties)

        _store_outputs(outputs, net, operation_id)


class StoreOutputsAction(sn.TransitionAction):
    def input_data(self, active_tokens_key, net):
        token_keys = self.connection.lrange(active_tokens_key, 0, -1)
        tokens = [sn.Token(self.connection, k) for k in token_keys]
        return sn.merge_token_data(tokens, "output")

    def execute(self, active_tokens_key, net, services):
        operation_id = self.args["operation_id"]
        input_data = self.input_data(active_tokens_key, net)

        _store_outputs(input_data, net, operation_id)


class GenomeNet(nb.EmptyNet):
    def __init__(self, builder, name, operation_id, input_connections):

        nb.EmptyNet.__init__(self, builder, name)

        self.operation_id = operation_id
        self.input_connections = input_connections
        self.flat_inputs = _flatten_input_connections(input_connections)

        self.start_transition = self.add_transition("%s start_trans" % name)
        self.success_transition = self.add_transition("%s success_trans" % name)
        self.failure_transition = self.add_transition("%s failure_trans" % name)

        self.failure_place = self.add_place("%s failure" % name)
        self.failure_place.arcs_out.add(self.failure_transition)


class GenomeActionNet(GenomeNet):
    def __init__(self, builder, name, operation_id, input_connections,
            action_type, action_id):

        GenomeNet.__init__(self, builder, name, operation_id, input_connections)

        self.action_type = action_type
        self.action_id = action_id

        self.success_place = self.add_place("%s success" % name)
        self.success_place.arcs_out.add(self.success_transition)

        args = {
            "action_type": self.action_type,
            "action_id": self.action_id,
            "with_outputs": True,
            "operation_id": self.operation_id,
            "input_connections": self.flat_inputs,
        }

        store_outputs_action = nb.ActionSpec(cls=StoreOutputsAction, args=args)

        self.shortcut = self.add_subnet(enets.LocalCommandNet,
                "%s shortcut" % name, action_class=GenomeShortcutAction,
                action_args=args)

        self.start_transition.arcs_out.add(self.shortcut.start)

        self.execute = self.add_subnet(enets.LSFCommandNet, "%s execute" % name,
                action_class=GenomeExecuteAction, action_args=args)

        self.shortcut.execute_success.action = store_outputs_action
        self.execute.execute_success.action = store_outputs_action

        self.bridge_places(self.shortcut.success, self.success_place, "")
        self.bridge_places(self.shortcut.failure, self.execute.start, "")

        self.bridge_places(self.execute.success, self.success_place, "")
        self.bridge_places(self.execute.failure, self.failure_place, "")


class GenomeParallelByNet(GenomeNet):
    def __init__(self, builder, name, operation_id, input_connections,
            action_type, action_id, parallel_by):

        GenomeNet.__init__(self, builder, name, operation_id, input_connections)

        self.action_type = action_type
        self.action_id = action_id
        self.parallel_by = parallel_by

        self.success_place = self.add_place("%s success" % name)
        self.success_place.arcs_out.add(self.success_transition)

        args = {
            "action_type": self.action_type,
            "action_id": self.action_id,
            "with_outputs": True,
            "operation_id": self.operation_id,
            "input_connections": self.flat_inputs,
        }

        store_outputs_action = nb.ActionSpec(cls=StoreOutputsAction, args=args)

        self.shortcut = self.add_subnet(enets.LocalCommandNet,
                "%s shortcut" % name, action_class=GenomeShortcutAction,
                action_args=args)

        self.start_transition.arcs_out.add(self.shortcut.start)

        self.execute = self.add_subnet(enets.LSFCommandNet, "%s execute" % name,
                action_class=GenomeExecuteAction, action_args=args)

        self.shortcut.execute_success.action = store_outputs_action
        self.execute.execute_success.action = store_outputs_action

        self.bridge_places(self.shortcut.success, self.success_place, "")
        self.bridge_places(self.shortcut.failure, self.execute.start, "")

        self.bridge_places(self.execute.success, self.success_place, "")
        self.bridge_places(self.execute.failure, self.failure_place, "")



class GenomeModelNet(GenomeNet):
    def __init__(self, builder, name, operation_id, input_connections):
        GenomeNet.__init__(self, builder, name, operation_id, input_connections)


class GenomeConvergeNet(nb.EmptyNet):
    def __init__(self, builder, name, operation_id, input_connections,
            input_property_order, output_properties):

        nb.EmptyNet.__init__(self, builder, name)

        self.input_property_order = input_property_order
        self.output_properties = output_properties
        self.operation_id = operation_id

        args = {
            "operation_id": self.operation_id,
            "with_outputs": True,
            "input_property_order": self.input_property_order,
            "output_properties": self.output_properties,
        }

        action = nb.ActionSpec(cls=GenomeConvergeAction, args=args)

        self.start_transition = self.add_transition("converge",
                action=action)
        self.success_transition = self.start_transition
