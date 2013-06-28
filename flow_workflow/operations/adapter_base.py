import os
import re

import logging


LOG = logging.getLogger(__name__)

MAX_FILENAME_LEN = 256


def clean_log_file_name(name):
    base = re.sub("[^A-Za-z0-9_.-]+", "_", name)[:MAX_FILENAME_LEN]
    return re.sub("^_*|_*$", "", base)

def parse_xml(xml):
    name = xml.attrib["name"]
    type_nodes = xml.findall("operationtype")
    if len(type_nodes) != 1:
        raise ValueError(
            "Wrong number of <operationtype> tags in operation %s" % name)

    type_node = type_nodes[0]
    operation_attributes = xml.attrib
    type_attributes = type_node.attrib
    return name, type_node, operation_attributes, type_attributes

def determine_log_paths(name, operation_id, log_dir):
    basename = clean_log_file_name(name)
    out_file = "%s.%s.out" % (basename, operation_id)
    err_file = "%s.%s.err" % (basename, operation_id)
    stdout_log_file = os.path.join(log_dir, out_file)
    stderr_log_file = os.path.join(log_dir, err_file)
    return stdout_log_file, stderr_log_file


class AdapterBase(object):
    def __init__(self, adapter_factory, operation_id, log_dir=None, parent=None, xml=None):
        self.adapter_factory = adapter_factory
        self.operation_id = operation_id
        self.parent = parent
        if parent is None:
            self.parent_id = None
        else:
            self.parent_id = self.parent.operation_id

        self.xml = xml
        self.name = 'UnnamedOperation'
        if xml is not None:
            name, type_node, operation_attributes, type_attributes = parse_xml(xml)
            self.name = name
            self._type_node = type_node
            self._operation_attributes = operation_attributes
            self._type_attributes = type_attributes

        self.log_dir = log_dir
        if log_dir is not None:
            stdout, stderr = determine_log_paths(self.name, operation_id, log_dir)
            self.stdout_log_file = stdout
            self.stderr_log_file = stderr

    def net(self, super_net, output_properties=None, input_connections=None,
            resources=None):
        raise NotImplementedError("net not implemented in %s" %
                                  self.__class__.__name__)

    @property
    def children(self):
        return []
