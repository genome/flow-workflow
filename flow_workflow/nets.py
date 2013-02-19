import flow.command_runner.executors.nets as enets
import flow.redisom as rom
import flow.petri.netbuilder as nb
import flow.petri.safenet as sn

from collections import namedtuple

import logging

LOG = logging.getLogger(__name__)

GENOME_WRAPPER = "workflow-wrapper"

ParallelBySpec = namedtuple("ParallelBySpec", "property index")
OperationSpec = namedtuple("OperationSpec", "type id")

def _output_variable_name(operation_id, output_name):
    return "_wf_outp_%d_%s" % (int(operation_id), output_name)


def _op_outputs_variable_name(operation_id):
    return "_wf_outp_%d" % int(operation_id)


def _store_outputs(outputs, net, operation_id):
    if not outputs:
        return

    keys = []
    for k, v in outputs.iteritems():
        keys.append(k)
        name = _output_variable_name(operation_id, k)
        net.set_variable(name, v)

    net.set_variable(_op_outputs_variable_name(operation_id), keys)


def _operation_outputs(net, operation_id):
    key = _op_outputs_variable_name(operation_id)
    return net.variable(key)


def _do_converge(inputs, input_property_order, output_properties):
    out_list = [inputs[x] for x in input_property_order]
    return {prop: out_list for prop in output_properties}


class InputsMixin(object):
    def _fetch_inputs(self, net, data_arcs):
        inputs = {}
        if data_arcs:
            for src_id, prop_hash in data_arcs.iteritems():
                if not prop_hash:
                    names = _operation_outputs(net, src_id)
                    prop_hash = {x: x for x in names}

                for dst_prop, src_prop in prop_hash.iteritems():
                    varname = _output_variable_name(src_id, src_prop)
                    value = net.variable(varname)
                    if value:
                        inputs[dst_prop] = value

        return inputs

    def input_data(self, active_tokens_key, net):
        inputs = self._fetch_inputs(net, self.args["input_connections"])
        parallel_by = self.args.get("parallel_by")
        parallel_by_idx = self.args.get("parallel_by_idx")
        if parallel_by and parallel_by_idx is not None:
            idx = self.args["parallel_by_idx"]
            inputs[parallel_by] = inputs[parallel_by][idx]

        return inputs


class BuildParallelByAction(InputsMixin, sn.TransitionAction):
    required_arguments = ["action_type", "action_id", "parallel_by",
            "input_connections", "operation_id"]

    def execute(self, active_tokens_key, net, services):
        action_type = self.args["action_type"]
        action_id = self.args["action_id"]
        parallel_by = self.args["parallel_by"]

        inputs = self.input_data(active_tokens_key, net)

        builder = nb.NetBuilder()
        pby_net = builder.add_subnet(nb.EmptyNet, self.name)

        pby_net.start_place = pby_net.add_place("start")
        pby_net.success_place = pby_net.add_place("success")
        pby_net.failing_place = pby_net.add_place("failing")
        pby_net.failure_place = pby_net.add_place("failure")

        args = {"operation_id": 0}
        store_outputs_action = nb.ActionSpec(cls=StoreOutputsAction, args=args)
        pby_net.start_transition = pby_net.add_transition("start",
                action=store_outputs_action)

        pby_net.success_transition = pby_net.add_transition("success")
        pby_net.failure_transition = pby_net.add_transition("failure")

        pby_net.start_place.arcs_out.add(pby_net.start_transition)
        pby_net.failing_place.arcs_out.add(pby_net.failure_transition)
        pby_net.success_transition.arcs_out.add(pby_net.success_place)
        pby_net.failure_transition.arcs_out.add(pby_net.failure_place)

        input_conns = {0: {x: x for x in inputs}}
        input_source_id = 0

        for i, value in enumerate(inputs[parallel_by]):
            op_id = i+1
            name = "%s (#%d)" % (self.name, op_id)

            pby_spec = ParallelBySpec(property=parallel_by, index=i)

            subnet = pby_net.add_subnet(GenomeActionNet,
                    name=name,
                    operation_id=op_id,
                    input_connections=input_conns,
                    action_type=action_type,
                    action_id=action_id,
                    parallel_by_spec=pby_spec)

            done = pby_net.add_place("%d done" % i)
            subnet.success_transition.arcs_out.add(done)
            subnet.failure_transition.arcs_out.add(done)

            done.arcs_out.add(pby_net.success_transition)
            done.arcs_out.add(pby_net.failure_transition)

            pby_net.bridge_transitions(pby_net.start_transition,
                    subnet.start_transition)

            pby_net.bridge_transitions(subnet.success_transition,
                    pby_net.success_transition)

            subnet.failure_transition.arcs_out.add(pby_net.failing_place)

        success_args = {"remote_net_key": net.key,
                "remote_place_id": self.place_refs[0],
                "data_type": "output"}

        failure_args = {"remote_net_key": net.key,
                "remote_place_id": self.place_refs[1],
                "data_type": "output"}

        pby_net.success_transition.action = nb.ActionSpec(
                cls=sn.SetRemoteTokenAction,
                args=success_args)

        pby_net.failure_transition.action = nb.ActionSpec(
                cls=sn.SetRemoteTokenAction,
                args=failure_args)

        stored_net = builder.store(self.connection)
        stored_net.copy_constants_from(net)

        orchestrator = services["orchestrator"]
        token = sn.Token.create(self.connection, data=inputs, data_type="output")
        orchestrator.set_token(stored_net.key, pby_net.start_place.index, token.key)


class GenomeShortcutAction(InputsMixin, enets.LocalDispatchAction):
    output_token_type = "input"
    required_arguments = ["operation_id", "action_type",
            "action_id"]

    def _command_line(self, net, input_data_key):
        return [GENOME_WRAPPER, self.args["action_type"], "shortcut",
                self.args["action_id"]]


class GenomeExecuteAction(InputsMixin, enets.LSFDispatchAction):
    output_token_type = "input"
    required_arguments = ["operation_id", "action_type",
            "action_id"]

    action_type = rom.Property(rom.String)

    def _command_line(self, net, input_data_key):
        return [GENOME_WRAPPER, self.args["action_type"], "execute",
                self.args["action_id"]]


class GenomeConvergeAction(InputsMixin, sn.TransitionAction):
    required_arguments = ["operation_id", "input_property_order",
            "output_properties"]

    output_token_type = "output"

    def execute(self, active_tokens_key, net, services):
        operation_id = self.args["operation_id"]
        input_property_order = self.args["input_property_order"]
        output_properties = self.args["output_properties"]

        input_data = self.input_data(active_tokens_key, net)

        outputs = _do_converge(input_data, input_property_order,
                output_properties)

        _store_outputs(outputs, net, operation_id)


class StoreOutputsAction(sn.TransitionAction):
    required_arguments = ["operation_id"]

    def input_data(self, active_tokens_key, net):
        token_keys = self.connection.lrange(active_tokens_key, 0, -1)
        tokens = [sn.Token(self.connection, k) for k in token_keys]
        merged = sn.merge_token_data(tokens, "output")
        return merged

    def execute(self, active_tokens_key, net, services):
        operation_id = self.args["operation_id"]
        input_data = self.input_data(active_tokens_key, net)

        _store_outputs(input_data, net, operation_id)


class GenomeNet(nb.EmptyNet):
    def __init__(self, builder, name, operation_id, input_connections):

        nb.EmptyNet.__init__(self, builder, name)

        self.operation_id = operation_id
        self.input_connections = input_connections

        self.start_transition = self.add_transition("%s start_trans" % name)
        self.success_transition = self.add_transition("%s success_trans" % name)
        self.failure_transition = self.add_transition("%s failure_trans" % name)

        self.failure_place = self.add_place("%s failure" % name)
        self.failure_place.arcs_out.add(self.failure_transition)


class GenomeModelNet(GenomeNet):
    pass


class GenomeActionNet(GenomeNet):
    def __init__(self, builder, name, operation_id, input_connections,
            action_type, action_id, parallel_by_spec=None):

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
            "input_connections": self.input_connections,
        }

        if parallel_by_spec:
            args["parallel_by"] = parallel_by_spec.property
            args["parallel_by_idx"] = parallel_by_spec.index

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


class GenomeParallelByNet(nb.EmptyNet):
    def __init__(self, builder, name, operation_id, input_connections,
            action_type, action_id, parallel_by):

        nb.EmptyNet.__init__(self, builder, name)

        self.parallel_by = parallel_by
        self.action_type = action_type
        self.action_id = action_id
        self.operation_id = operation_id
        self.input_connections = input_connections

        args = {
            "action_type": self.action_type,
            "action_id": self.action_id,
            "with_outputs": True,
            "operation_id": self.operation_id,
            "input_connections": self.input_connections,
            "parallel_by": parallel_by
        }

        self.running = self.add_place("running")
        self.on_success = self.add_place("on_success")
        self.on_failure = self.add_place("on_failure")
        place_refs = [self.on_success.index, self.on_failure.index]

        action = nb.ActionSpec(cls=BuildParallelByAction, args=args,
                place_refs=place_refs)
        self.start_transition = self.add_transition("start_transition",
                action=action)

        self.success_transition = self.add_transition("success_transition")
        self.failure_transition = self.add_transition("failure_transition")

        self.start_transition.arcs_out.add(self.running)
        self.running.arcs_out.add(self.success_transition)
        self.running.arcs_out.add(self.failure_transition)

        self.on_success.arcs_out.add(self.success_transition)
        self.on_failure.arcs_out.add(self.failure_transition)


class GenomeConvergeNet(nb.EmptyNet):
    def __init__(self, builder, name, operation_id, input_connections,
            input_property_order, output_properties):

        nb.EmptyNet.__init__(self, builder, name)

        self.input_property_order = input_property_order
        self.output_properties = output_properties
        self.operation_id = operation_id
        self.input_connections = input_connections

        args = {
            "operation_id": self.operation_id,
            "with_outputs": True,
            "input_property_order": self.input_property_order,
            "output_properties": self.output_properties,
            "input_connections": self.input_connections,
        }

        action = nb.ActionSpec(cls=GenomeConvergeAction, args=args)

        self.start_transition = self.add_transition("converge",
                action=action)
        self.success_transition = self.start_transition
