def op_outputs_variable_name(operation_id, parallel_idx):
    return "_wf_outp_%d_%d" % (int(operation_id), int(parallel_idx))


#def op_exit_code_variable_name(operation_id, parallel_idx):
#    return "_wf_exit_%d_%d" % (int(operation_id), int(parallel_idx))


def operation_output_names(net, operation_id, parallel_idx):
    key = op_outputs_variable_name(operation_id, parallel_idx)
    return net.variable(key)


def output_variable_name(operation_id, output_name, parallel_idx):
    return "_wf_outp_%d_%s_%d" % (int(operation_id), output_name,
            int(parallel_idx))

#def output_variable_name(operation_id, output_name, parallel_indices):
#    parallel_indices_string = json.dumps(parallel_indices, sort_keys=True)
#    return '_wf_var_%d_%s_%s' % (operation_id, output_name,
#            parallel_indices_string)
