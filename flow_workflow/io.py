import logging

LOG = logging.getLogger(__name__)

def extract_workflow_data(tokens):
    outputs = {}

    for token in tokens:
        tok_outputs = token.data.get('workflow_data', {})
        outputs.update(tok_outputs)

    return outputs


def load_input(net, input_connections, name=None, parallel_id=None):
    """
    Return the input <name>d from the <net> for an operation when given its
    <input_connections> and <parallel_id>.  If name is None then all
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
                        parallel_id=parallel_id)
                if name == dst_prop:
                    return value
                inputs[dst_prop] = value

    if name is not None:
        raise KeyError("Input %s not found in input_connections (%s)" %
                (name, input_connections))
    else:
        return inputs

def load_output(net, operation_id, property, parallel_id=None):
    varname = _output_variable_name(operation_id=operation_id,
            property=property, parallel_id=parallel_id)
    value = net.variable(varname)
    return value


def store_output(net, operation_id, property, value, parallel_id=None):
    varname = _output_variable_name(operation_id=operation_id,
            property=property,
            parallel_id=parallel_id)
    LOG.debug("Setting output (%s) from operation (%s) on net (%s) via %s = %s",
            property, operation_id, net.key, varname, value)

    net.set_variable(varname, value)


def store_outputs(net, operation_id, outputs, parallel_id=None):
    if not outputs:
        return

    for name, value in outputs.iteritems():
        store_output(net=net,
                operation_id=operation_id,
                property=name,
                value=value,
                parallel_id=parallel_id)

def _output_variable_name(operation_id, property, parallel_id=None):
    """
    Operation outputs are stored on the net.  This constructs the name of
    the variable where they are stored.
    """
    base = "_wf_outp_%s_%s" % (int(operation_id), property)
    if parallel_id is None:
        return base
    else:
        parallel_part = ''
        for key in sorted(parallel_id.keys()):
            parallel_part += '|%s:%s' % (key, parallel_id[key])

        return base + parallel_part


