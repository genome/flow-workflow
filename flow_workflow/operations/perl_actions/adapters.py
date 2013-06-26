from flow_workflow.workflow_parts.base import WorkflowOperation
from flow_workflow.petri_net.future_nets.perl_action import (GenomeCommandNet,
        GenomeEventNet)
from flow_workflow.petri_net.future_nets.parallel_by import GenomeParallelByNet

def net_class_kwargs(operation, input_connections, resources):
    return {
            'name':operation.name,
            'operation_id':operation.operation_id,
            'input_connections':input_connections,
            'stderr':operation.stderr_log_file,
            'sdtout':operation.stdout_log_file,
            'resources':resources,
            'action_id':operation.action_id,
            'remote_execution':operation.remote_execution,
            'project_name':operation.project_name,
            'parent_operation_id':operation.parent.operation_id,
    }

class CommandOperation(WorkflowOperation):
    @property
    def parallel_by(self):
        if "parallelBy" in self._operation_attributes:
            return self._operation_attributes["parallelBy"]
        else:
            return None

    @property
    def action_id(self):
        self.action_id = self._type_attributes['commandClass']

    def net(self, super_net, input_connections=None, output_properties=None,
            resources=None):
        kwargs = net_class_kwargs(self, input_connections, resources)
        if self.parallel_by is not None:
            this_net = GenomeCommandNet(**kwargs)
            return super_net.add_subnet(GenomeParallelByNet,
                    target_net=this_net,
                    parallel_property=self.parallel_by)
        else:
            return super_net.add_subnet(GenomeCommandNet, **kwargs)

class EventOperation(WorkflowOperation):
    @property
    def action_id(self):
        self.action_id = self._type_attributes['event_id']

    def net(self, super_net, input_connections=None, output_properties=None,
            resources=None):
        kwargs = net_class_kwargs(self, input_connections, resources)
        return super_net.add_subnet(GenomeEventNet, **kwargs)
