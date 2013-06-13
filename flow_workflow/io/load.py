from flow_workflow.io import common

import itertools
import logging


LOG = logging.getLogger(__name__)


def get_workflow_outputs(net):
    # This is used by submit workflow to return outputs back to submitting perl
    LOG.info("Fetching outputs for net %s", net.key)
    workflow_id = net.constant('workflow_id')

    return operation_outputs(net, workflow_id, 0)


def operation_outputs(net, op_id, parallel_idx):
    output_names = common.operation_output_names(net, op_id, parallel_idx)
    variable_names = [common.output_variable_name(op_id, output_name,
        parallel_idx) for output_name in output_names]

    outputs = {output_name: net.variable(variable_name)
            for output_name, variable_name in itertools.izip(
                output_names, variable_names)}
    return outputs


def extract_data_from_tokens(tokens):
    outputs = {}

    for token in tokens:
        tok_outputs = token.data.get("outputs", {})
        outputs.update(tok_outputs)

    return outputs


def action_inputs(net, action, parallel_idx):
    input_connections = action.args.get('input_connections', {})
    return collect_inputs(net, input_connections, parallel_idx)


def collect_inputs(net, input_connections, parallel_idx):
    inputs = {}
    for src_id, prop_hash in input_connections.iteritems():
        if not prop_hash:
            names = common.operation_output_names(net, src_id, parallel_idx)
            prop_hash = {x: x for x in names}

        for dst_prop, src_prop in prop_hash.iteritems():
            varname = common.output_variable_name(
                    src_id, src_prop, parallel_idx)
            value = net.variable(varname)
            inputs[dst_prop] = value

    return inputs
