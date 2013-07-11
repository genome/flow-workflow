import logging


LOG = logging.getLogger(__name__)


def extract_workflow_data(net, token_indices):
    outputs = {}

    for token_index in token_indices:
        token = net.token(token_index)
        tok_outputs = token.data.get('workflow_data', {})
        outputs.update(tok_outputs)

    return outputs


def load_input(net, input_connections, property_name, parallel_id=None):
    for src_id, prop_hash in input_connections.iteritems():
        if property_name in prop_hash:
            return load_output(net=net, operation_id=src_id,
                               property_name=prop_hash[property_name],
                               parallel_id=parallel_id)

    # XXX I'm not sure we should raise a key error here, but the old code did.
    raise KeyError("Input %s not found in input_connections (%s)" %
            (property_name, input_connections))


def load_inputs(net, input_connections, parallel_id=None):
    inputs = {}
    for src_id, prop_hash in input_connections.iteritems():
        for dest_prop_name, src_prop_name in prop_hash.iteritems():
            inputs[dest_prop_name] = load_output(net=net, operation_id=src_id,
                    property_name=src_prop_name, parallel_id=parallel_id)

    return inputs


def load_output(net, operation_id, property_name, parallel_id=None):
    LOG.debug('load_output(%r, %r, %r)',
            operation_id, property_name, parallel_id)
    if parallel_id:
        iter_over_parallel_id = list(parallel_id)
    else:
        iter_over_parallel_id = []

    while iter_over_parallel_id:
        varname = _output_variable_name(operation_id=operation_id,
                property_name=property_name, parallel_id=iter_over_parallel_id)
        try:
            return net.variables[varname]
        except KeyError:
            iter_over_parallel_id.pop()

    varname = _output_variable_name(operation_id=operation_id,
            property_name=property_name, parallel_id=iter_over_parallel_id)

    return net.variables[varname]


def load_outputs(net, operation_id, property_names, parallel_id=None):
    outputs = {}
    for output_name in property_names:
        outputs[output_name] = load_output(net=net, operation_id=operation_id,
                property_name=output_name, parallel_id=parallel_id)
    return outputs


def store_output(net, operation_id, property_name, value, parallel_id=None):
    LOG.debug('store_output(%r, %r, %r, %r)',
            operation_id, property_name, value, parallel_id)
    varname = _output_variable_name(operation_id=operation_id,
            property_name=property_name, parallel_id=parallel_id)

    net.set_variable(varname, value)


def store_outputs(net, operation_id, outputs, parallel_id=None):
    if not outputs:
        return

    for name, value in outputs.iteritems():
        store_output(net=net,
                operation_id=operation_id,
                property_name=name,
                value=value,
                parallel_id=parallel_id)

def _output_variable_name(operation_id, property_name, parallel_id=None):
    """
    Operation outputs are stored on the net.  This constructs the name of
    the variable where they are stored.
    """
    base = "_wf_outp_%s_%s" % (int(operation_id), property_name)

    if parallel_id:
        parallel_part = '|' + '|'.join('%s:%s' % (op_id, par_idx)
                for op_id, par_idx in parallel_id)
    else:
        parallel_part = ''

    return base + parallel_part
