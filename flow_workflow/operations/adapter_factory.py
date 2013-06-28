from flow_workflow.operations.connectors.adapters import (InputConnectorAdapter,
        OutputConnectorAdapter)
from flow_workflow.operations.block.adapter import BlockAdapter
from flow_workflow.operations.converge.adapter import ConvergeAdapter
from flow_workflow.operations.model.adapter import ModelAdapter
from flow_workflow.operations.perl_actions.adapters import (CommandAdapter,
        EventAdapter)
from flow_workflow.operations.adapter_base import parse_xml

import logging


LOG = logging.getLogger(__name__)

OPERATION_TYPES = {
    "InputConnector": InputConnectorAdapter,
    "OutputConnector": OutputConnectorAdapter,
    "Workflow::OperationType::Converge": ConvergeAdapter,
    "Workflow::OperationType::Block": BlockAdapter,
    "Workflow::OperationType::Command": CommandAdapter,
    "Workflow::OperationType::Event": EventAdapter,
    "Workflow::OperationType::Model": ModelAdapter,
}

class AdapterFactory(object):
    def __init__(self, operation_types=OPERATION_TYPES):
        self._operation_types = operation_types
        self.next_operation_id = 0

    def create_from_xml(self, xml, **adapter_class_kwargs):
        _, _, _, type_attributes = parse_xml(xml)
        type_class = type_attributes['typeClass']
        return self.create(type_class, xml=xml, **adapter_class_kwargs)

    def create(self, type_class, **adapter_class_kwargs):
        if type_class not in self._operation_types:
            raise ValueError("Unknown operation type %s in workflow xml" %
                               type_class)

        cls = self._operation_types[type_class]
        operation_id = self.next_operation_id
        self.next_operation_id += 1
        return cls(adapter_factory=self, operation_id=operation_id, **adapter_class_kwargs)
