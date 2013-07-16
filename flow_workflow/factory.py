from flow.util.containers import head
import pkg_resources
import re


MODULE = None


_NEXT_OPERATION_ID = 0


def adapter(operation_type, *args, **kwargs):
    global _NEXT_OPERATION_ID
    ep = head(pkg_resources.iter_entry_points('flow_workflow.adapters',
        sanitize_operation_type(operation_type)))
    cls = ep.load()
    obj = cls(operation_id=_NEXT_OPERATION_ID, *args, **kwargs)
    _NEXT_OPERATION_ID += 1

    return obj


def adapter_from_xml(xml, *args, **kwargs):
    return adapter(get_operation_type(xml), *args, xml=xml, **kwargs)


def get_operation_type(xml):
    operation_type_node = xml.find('operationtype')
    return operation_type_node.attrib['typeClass']


def sanitize_operation_type(operation_type_string):
    return re.sub(' ', '_',
            re.sub('^Workflow::OperationType::', '', operation_type_string))


def load_operation(net, operation_id):
    operation_dict = net.variables[operation_variable_name(operation_id)]

    ep = head(pkg_resources.iter_entry_points(
        'flow_workflow.operations', operation_dict.pop('_class')))
    cls = ep.load()
    return cls(**operation_dict)


def operation_variable_name(operation_id):
    return '_wf_op_%d' % operation_id
