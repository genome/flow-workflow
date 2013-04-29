from flow import petri
from twisted.internet import defer
import flow.redisom as rom

import logging

LOG = logging.getLogger(__name__)


def get_workflow_outputs(net):
    LOG.info("Fetching outputs for net %s", net.key)
    workflow_id = net.constant('workflow_id')
    output_names = operation_output_names(net, workflow_id)

    result = {}
    if output_names:
        for name in output_names:
            munged_name = output_variable_name(workflow_id, name)
            result[name] = net.variable(munged_name)

    return result


def output_variable_name(operation_id, output_name, parallel_idx=0):
    return "_wf_outp_%d_%s_%d" % (int(operation_id), output_name,
            int(parallel_idx))


def op_outputs_variable_name(operation_id, parallel_idx=0):
    return "_wf_outp_%d_%d" % (int(operation_id), int(parallel_idx))


def op_exit_code_variable_name(operation_id, parallel_idx=0):
    return "_wf_exit_%d_%d" % (int(operation_id), int(parallel_idx))


def store_outputs(outputs, net, operation_id, parallel_idx=0):
    if not outputs:
        return

    keys = []
    for k, v in outputs.iteritems():
        keys.append(k)
        name = output_variable_name(operation_id, k, parallel_idx)
        LOG.debug("Setting net (%s) variables %r=%r", net.key, name, v)
        net.set_variable(name, v)

    net.set_variable(op_outputs_variable_name(operation_id, parallel_idx), keys)


def operation_output_names(net, operation_id, parallel_idx=0):
    key = op_outputs_variable_name(operation_id, parallel_idx)
    return net.variable(key)


def operation_outputs(net, op_id, parallel_idx=0):
    names = operation_output_names(net, op_id, parallel_idx)
    outputs = {x: net.variable(output_variable_name(op_id, x, parallel_idx))
            for x in names}
    return outputs


class InputsMixin(object):
    required_arguments = ["input_connections"]

    def _fetch_inputs(self, net, data_arcs, parallel_idx):
        if data_arcs is None:
            return {}

        inputs = {}
        for src_id, prop_hash in data_arcs.iteritems():
            if not prop_hash:
                names = operation_output_names(net, src_id)
                prop_hash = {x: x for x in names}

            for dst_prop, src_prop in prop_hash.iteritems():
                varname = output_variable_name(src_id, src_prop, parallel_idx)
                value = net.variable(varname)
                inputs[dst_prop] = value

        return inputs

    def input_data(self, active_tokens_key, net):
        input_connections = self.args.get("input_connections")
        tokens = self.tokens(active_tokens_key)
        try:
            parallel_index = tokens[0].color_idx.value
        except rom.NotInRedisError:
            parallel_index = 0

        inputs = self._fetch_inputs(net, input_connections, parallel_index)
        LOG.debug("Inputs: %r", inputs)

        return inputs


class StoreOutputsAction(petri.TransitionAction):
    required_arguments = ["operation_id"]
    optional_arguments = ["parallel_by"]

    def wf_input_data(self, tokens, net):
        outputs = {}
        exit_codes = []
        for token in tokens:
            LOG.debug("StoreOutputsAction: token %s has data %r", token.key,
                    token.data.value)
            tok_outputs = token.data.get("outputs", {})
            outputs.update(tok_outputs)
            exit_code = token.data.get("exit_code")
            if exit_code is not None:
                exit_codes.append(exit_code)

        LOG.debug("%s: exit code %r", self.name, exit_codes)
        if len(exit_codes) == 1:
            vname = op_exit_code_variable_name(self.args["operation_id"])
            net.set_variable(vname, exit_codes[0])

        return outputs

    def execute(self, active_tokens_key, net, service_interfaces):
        operation_id = self.args["operation_id"]
        tokens = self.tokens(active_tokens_key)
        input_data = self.wf_input_data(tokens, net)
        try:
            parallel_idx = tokens[0].color_idx.value
        except rom.NotInRedisError:
            parallel_idx = 0

        LOG.debug("%s (%s/%d color=%r) storing outputs: %r", self.name, net.key,
                operation_id, parallel_idx, input_data)

        store_outputs(input_data, net, operation_id, parallel_idx)
        return defer.succeed(None)


class StoreInputsAsOutputsAction(InputsMixin, petri.TransitionAction):
    required_arguments = InputsMixin.required_arguments + ["operation_id"]

    def execute(self, active_tokens_key, net, service_interfaces):
        operation_id = self.args["operation_id"]
        input_data = self.input_data(active_tokens_key, net)

        LOG.debug("StoreInputsAsOutputsAction on net %s, op %d, inputs: %r",
                net.key, int(operation_id), input_data)

        store_outputs(input_data, net, operation_id)
        return defer.succeed(None)
