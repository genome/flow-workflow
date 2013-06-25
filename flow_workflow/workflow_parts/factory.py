from flow_workflow.workflow_parts.connectors import (InputConnector,
        OutputConnector)
from flow_worklow.workflow_parts.converge import ConvergeOperation
from flow_worklow.workflow_parts.block import BlockOperation
from flow_worklow.workflow_parts.perl_action import (CommandOperation,
        EventOperation)
from flow_worklow.workflow_parts.model import ModelOperation

class WorkflowPartFactory(object):
    _instance = None

    @classmethod
    def get_instance(cls):
        if cls._instance is None:
            cls._instance = WorkflowPartFactory()
        return cls._instance

    def __init__(self):
        self.next_operation_id = 0
        self._operation_types = {
            "InputConnector": InputConnector,
            "OutputConnector": OutputConnector,
            "Workflow::OperationType::Converge": ConvergeOperation,
            "Workflow::OperationType::Block": BlockOperation,
            "Workflow::OperationType::Command": CommandOperation,
            "Workflow::OperationType::Event": EventOperation,
            "Workflow::OperationType::Model": ModelOperation,
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
        return cls(part_factory=self, operation_id=operation_id, **kwargs)
