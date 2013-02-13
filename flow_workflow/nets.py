import flow.command_runner.executors.nets as enets
import flow.redisom as rom
import flow.petri.netbuilder as nb
import flow.petri.safenet as sn

import logging

LOG = logging.getLogger(__name__)

GENOME_WRAPPER = "workflow-wrapper"

def output_variable_name(job_number, output_name):
    return "_wf_outp_%d_%s" % (job_number, output_name)


def _flatten_input_connections(input_connections):
    flat = {}
    for src, props in input_connections.iteritems():
        for dst_prop, src_prop in props.iteritems():
            src_name = output_variable_name(src, src_prop)
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


class GenomeConvergeAction(sn.TransitionAction, InputsMixin):
    output_token_type = "output"

    input_property_order = rom.Property(rom.List)
    output_properties = rom.Property(rom.List)

    def execute(self, input_data, net, services):
        out = [input_data[x] for x in self.input_property_order.value]
        return {prop: out for prop in self.output_properties.value}


class StoreOutputsAction(sn.TransitionAction):
    def input_data(self, active_tokens_key, net):
        token_keys = self.connection.lrange(active_tokens_key, 0, -1)
        tokens = [sn.Token(self.connection, k) for k in token_keys]
        return sn.merge_token_data(tokens, "output")

    def execute(self, active_tokens_key, net, services):
        job_number = self.args["job_number"]
        input_data = self.input_data(active_tokens_key, net)

        for k, v in input_data.iteritems():
            name = output_variable_name(job_number, k)
            net.set_variable(name, v)


class GenomeActionNet(nb.EmptyNet):
    def __init__(self, builder, name, job_number, action_type, action_id,
            input_connections):

        flat_input_connections = _flatten_input_connections(input_connections)
        nb.EmptyNet.__init__(self, builder, name)

        self.start_transition = self.add_transition("%s start_trans" % name)
        self.success_transition = self.add_transition("%s success_trans" % name)
        self.failure_transition = self.add_transition("%s failure_trans" % name)

        self.success_place = self.add_place("%s success" % name)
        self.failure_place = self.add_place("%s failure" % name)

        args = {
            "action_type": action_type,
            "action_id": action_id,
            "with_outputs": True,
            "job_number": job_number,
            "input_connections": flat_input_connections,
        }

        self.shortcut = builder.add_subnet(enets.LocalCommandNet, "%s shortcut" % name,
                action_class=GenomeShortcutAction,
                action_args=args)

        self.shortcut.execute_success.action_class = StoreOutputsAction
        self.shortcut.execute_success.action_args = args

        self.start_transition.arcs_out.add(self.shortcut.start)

        self.execute = builder.add_subnet(enets.LSFCommandNet, "%s execute" % name,
                action_class=GenomeExecuteAction,
                action_args=args)

        self.execute.execute_success.action_class = StoreOutputsAction
        self.execute.execute_success.action_args = args

        builder.bridge_places(self.shortcut.success, self.success_place)
        builder.bridge_places(self.shortcut.failure, self.execute.start)

        builder.bridge_places(self.execute.success, self.success_place)
        builder.bridge_places(self.execute.failure, self.failure_place)

        self.success_place.arcs_out.add(self.success_transition)
        self.failure_place.arcs_out.add(self.failure_transition)
