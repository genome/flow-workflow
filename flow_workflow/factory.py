import pkg_resources
import re


MODULE = None


_NEXT_OPERATION_ID = 0


def adapter(operation_type, *args, **kwargs):
    global _NEXT_OPERATION_ID
    for ep in pkg_resources.iter_entry_points('flow_workflow.adapters',
            sanitize_operation_type(operation_type)):
        cls = ep.load()
        obj = cls(operation_id=_NEXT_OPERATION_ID, *args, **kwargs)
        _NEXT_OPERATION_ID += 1
        return obj
    else:
        raise RuntimeError('Could not find adapter for operation type: %s (%s)'
                % (operation_type, sanitize_operation_type(operation_type)))


def adapter_from_xml(xml, *args, **kwargs):
    return adapter(get_operation_type(xml), *args, xml=xml, **kwargs)


def get_operation_type(xml):
    operation_type_node = xml.find('operationtype')
    return operation_type_node.attrib['typeClass']


def sanitize_operation_type(operation_type_string):
    return re.sub(' ', '_',
            re.sub('^Workflow::OperationType::', '', operation_type_string))


# XXX use pkg_resources
def load_operation(net, operation_id):
    if operation_id is None:
        # XXX Is this the behavior we want?
        return NullOperation()

    operation_dict = net.variables[operation_variable_name(operation_id)]
    cls = getattr(MODULE, operation_dict.pop('_class'))
    return cls(**operation_dict)


def operation_variable_name(operation_id):
    return '_wf_op_%d' % operation_id
