from flow_workflow.operations.connectors.adapters import (InputConnectorAdapter,
        OutputConnectorAdapter)
from flow_worklow.operations.block.adapter import BlockAdapter
from flow_worklow.operations.converge.adapter import ConvergeAdapter
from flow_worklow.operations.model.adapter import ModelAdapter
from flow_worklow.operations.perl_actions.adapters import (CommandAdapter,
        EventAdapter)

import logging


LOG = logging.getLogger(__name__)

class AdapterFactory(object):
    def __init__(self):
        self.next_operation_id = 0
        self._operation_types = {
            "InputConnector": InputConnectorAdapter,
            "OutputConnector": OutputConnectorAdapter,
            "Workflow::OperationType::Converge": ConvergeAdapter,
            "Workflow::OperationType::Block": BlockAdapter,
            "Workflow::OperationType::Command": CommandAdapter,
            "Workflow::OperationType::Event": EventAdapter,
            "Workflow::OperationType::Model": ModelAdapter,
        }


    def create_from_xml(self, xml, **kwargs):
        optype_tags = xml.findall("operationtype")
        name = xml.attrib["name"]
        if len(optype_tags) != 1:
            raise ValueError(
                    "Wrong number of <operationtype> subtags (%d) in "
                    "operation %s" % (len(optype_tags), name))

        optype = optype_tags[0]
        type_class = optype.attrib["typeClass"]
        return self.create(type_class, xml=xml, **kwargs)

    def create(self, type_class, **kwargs):
        if type_class not in self._operation_types:
            raise ValueError("Unknown operation type %s in workflow xml" %
                               type_class)

        cls = self._operation_types[type_class]
        operation_id = self.next_operation_id
        self.next_operation_id += 1
        return cls(adapter_factory=self, operation_id=operation_id, **kwargs)
