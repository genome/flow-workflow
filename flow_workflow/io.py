import logging

LOG = logging.getLogger(__name__)

def extract_workflow_data(tokens):
    outputs = {}

    for token in tokens:
        tok_outputs = token.data.get('workflow_data', {})
        outputs.update(tok_outputs)

    return outputs


def load_input(net, input_connections, name=None, parallel_idx=None):
    """
    Return the input <name>d from the <net> for an operation when given its
    <input_connections> and <parallel_idx>.  If name is None then all
    inputs are returned as a dictionary keyed on name.

    input_connections[src_operation_id][property] = src_property
    """
    inputs = {}
    for src_id, prop_hash in input_connections.iteritems():
        for dst_prop, src_prop in prop_hash.iteritems():
            if name is None or name == dst_prop:
                value = load_output(
                        net=net,
                        operation_id=src_id,
                        property=src_prop,
                        parallel_idx=parallel_idx)
                if name == dst_prop:
                    return value
                inputs[dst_prop] = value

    if name is not None:
        raise KeyError("Input %s not found in input_connections (%s)" %
                (name, input_connections))
    else:
        return inputs

def load_output(net, operation_id, property, parallel_idx=None):
    varname = _output_variable_name(operation_id=operation_id,
            property=property, parallel_idx=parallel_idx)
    value = net.variable(varname)
    return value


def store_output(net, operation_id, property, value, parallel_idx=None):
    varname = _output_variable_name(operation_id=operation_id,
            property=property,
            parallel_idx=parallel_idx)
    LOG.debug("Setting output (%s) from operation (%s) on net (%s) via %s = %s",
            property, operation_id, net.key, varname, value)

    net.set_variable(varname, value)


def store_outputs(net, operation_id, outputs, parallel_idx=None):
    if not outputs:
        return

    for name, value in outputs.iteritems():
        store_output(net=net,
                operation_id=operation_id,
                property=name,
                value=value,
                parallel_idx=parallel_idx)

def _output_variable_name(operation_id, property, parallel_idx=None):
    """
    Operation outputs are stored on the net.  This constructs the name of
    the variable where they are stored.
    """
    base = "_wf_outp_%s_%s" % (int(operation_id), property)
    if parallel_idx is None:
        return base
    else:
        return base + "_%s" % int(parallel_idx)


