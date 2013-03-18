import flow.petri.safenet as sn

import logging

LOG = logging.getLogger(__name__)


def get_workflow_outputs(net):
    LOG.info("Fetching outputs for net %s", net.key)
    workflow_id = net.constant('workflow_id')
    output_names = _operation_outputs(net, workflow_id)

    result = {}
    if output_names:
        for name in output_names:
            munged_name = output_variable_name(workflow_id, name)
            result[name] = net.variable(munged_name)

    return result


def output_variable_name(operation_id, output_name):
    return "_wf_outp_%d_%s" % (int(operation_id), output_name)


def op_outputs_variable_name(operation_id):
    return "_wf_outp_%d" % int(operation_id)


def op_exit_code_variable_name(operation_id):
    return "_wf_exit_%d" % int(operation_id)


def store_outputs(outputs, net, operation_id):
    if not outputs:
        return

    keys = []
    for k, v in outputs.iteritems():
        keys.append(k)
        name = output_variable_name(operation_id, k)
        net.set_variable(name, v)

    net.set_variable(op_outputs_variable_name(operation_id), keys)


def _operation_outputs(net, operation_id):
    key = op_outputs_variable_name(operation_id)
    return net.variable(key)


class InputsMixin(object):
    def _fetch_inputs(self, net, data_arcs):
        if data_arcs is None:
            return {}

        inputs = {}
        for src_id, prop_hash in data_arcs.iteritems():
            if not prop_hash:
                names = _operation_outputs(net, src_id)
                prop_hash = {x: x for x in names}

            for dst_prop, src_prop in prop_hash.iteritems():
                varname = output_variable_name(src_id, src_prop)
                value = net.variable(varname)
                if value:
                    inputs[dst_prop] = value

        return inputs

    def input_data(self, active_tokens_key, net):
        inputs = self._fetch_inputs(net, self.args.get("input_connections"))
        parallel_by = self.args.get("parallel_by")
        parallel_by_idx = self.args.get("parallel_by_idx")
        if parallel_by and parallel_by_idx is not None:
            idx = self.args["parallel_by_idx"]
            inputs[parallel_by] = inputs[parallel_by][idx]

        return inputs


class StoreOutputsAction(sn.TransitionAction):
    required_arguments = ["operation_id"]

    def input_data(self, active_tokens_key, net):
        token_keys = self.connection.lrange(active_tokens_key, 0, -1)
        tokens = [sn.Token(self.connection, k) for k in token_keys]
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
        input_data = self.input_data(active_tokens_key, net)

        LOG.debug("%s storing outputs: %r", self.name, input_data)

        store_outputs(input_data, net, operation_id)


class StoreInputsAsOutputsAction(InputsMixin, sn.TransitionAction):
    required_arguments = ["operation_id", "input_connections"]

    def execute(self, active_tokens_key, net, service_interfaces):
        operation_id = self.args["operation_id"]
        input_data = self.input_data(active_tokens_key, net)

        LOG.debug("StoreInputsAsOutputsAction on net %s, op %d, inputs: %r",
                net.key, int(operation_id), input_data)

        store_outputs(input_data, net, operation_id)
