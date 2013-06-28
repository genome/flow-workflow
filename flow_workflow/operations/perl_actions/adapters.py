from flow_workflow.operations.adapter_base import AdapterBase
from flow_workflow.operations.perl_actions.future_nets import (CommandNet,
        EventNet)
from flow_workflow.operations.perl_actions.parallel_by_net import ParallelByNet

class PerlActionAdapterBase(AdapterBase):
    def net_class_kwargs(self, input_connections, resources):
        return {
                'name':self.name,
                'operation_id':self.operation_id,
                'input_connections':input_connections,
                'stderr':self.stderr_log_file,
                'sdtout':self.stdout_log_file,
                'resources':resources,
                'action_id':self.action_id,
                'remote_execution':self.remote_execution,
                'project_name':self.project_name,
                'parent_operation_id':self.parent.operation_id,
        }

class CommandAdapter(PerlActionAdapterBase):
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
        kwargs = self.net_class_kwargs(input_connections, resources)
        if self.parallel_by is not None:
            this_net = CommandNet(**kwargs)
            return super_net.add_subnet(ParallelByNet,
                    target_net=this_net,
                    parallel_property=self.parallel_by)
        else:
            return super_net.add_subnet(CommandNet, **kwargs)

class EventAdapter(PerlActionAdapterBase):
    @property
    def action_id(self):
        self.action_id = self._type_attributes['event_id']

    def net(self, super_net, input_connections=None, output_properties=None,
            resources=None):
        kwargs = self.net_class_kwargs(input_connections, resources)
        return super_net.add_subnet(GenomeEventNet, **kwargs)
