import os
import re

import logging


LOG = logging.getLogger(__name__)

def log_file_name(name):
    base = re.sub("[^A-Za-z0-9_.-]+", "_", name)[:MAX_FILENAME_LEN]
    return re.sub("^_*|_*$", "", base)

class AdapterBase(object):
    def __init__(self, adapter_factory, operation_id, xml, log_dir, parent):
        self.adapter_factory = adapter_factory
        self.operation_id = operation_id
        self.parent = parent
        self.parent_id = self.parent.operation_id if parent else None

        self.xml = xml
        type_nodes = xml.findall("operationtype")
        self.name = xml.attrib["name"]
        if len(type_nodes) != 1:
            raise ValueError(
                "Wrong number of <operationtype> tags in operation %s" %
                self.name
        )

        self._type_node = type_nodes[0]
        self._operation_attributes = xml.attrib
        self._type_attributes = self._type_node.attrib

        self.log_dir = log_dir
        basename = log_file_name(self.name)
        out_file = "%s.%d.out" % (basename, operation_id)
        err_file = "%s.%d.err" % (basename, operation_id)
        self.stdout_log_file = os.path.join(log_dir, out_file)
        self.stderr_log_file = os.path.join(log_dir, err_file)


    def net(self, super_net, input_connections=None):
        raise NotImplementedError("net not implemented in %s" %
                                  self.__class__.__name__)

    @property
    def children(self):
        return []
