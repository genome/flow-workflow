import pkg_resources
import re


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
