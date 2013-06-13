from flow_workflow.io import common

import logging


LOG = logging.getLogger(__name__)


def store_variable(net, operation_id, parallel_idx, name, value):
    key = common.output_variable_name(operation_id, name, parallel_idx)
    LOG.debug("Setting net (%s) variables %r=%r", net.key, key, value)

    net.set_variable(key, value)


def store_outputs(outputs, net, operation_id, parallel_idx):
    if not outputs:
        return

    for k, v in outputs.iteritems():
        store_variable(net, operation_id, parallel_idx, k, v)

    net.set_variable(common.op_outputs_variable_name(
        operation_id, parallel_idx), outputs.keys())
