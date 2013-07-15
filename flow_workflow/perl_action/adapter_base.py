from flow_workflow.perl_action import actions
from flow_workflow.perl_action import future_nets
from flow_workflow.log_manager import LogManager

import abc
import flow_workflow.adapter_base


class PerlActionAdapterBase(flow_workflow.adapter_base.XMLAdapterBase):
    def __init__(self, *args, **kwargs):
        flow_workflow.adapter_base.XMLAdapterBase.__init__(self, *args, **kwargs)
        self.log_manager = LogManager(log_dir=self.log_dir,
                operation_id=self.operation_id,
                operation_name=self.name)

    # XXX action_type and action_id should be refactored into a data clump
    @abc.abstractproperty
    def action_type(self):
        pass

    @abc.abstractproperty
    def action_id(self):
        pass

    def net(self, input_connections, output_properties, resources):
        if self.parallel_by:
            return self._parallel_by_net(input_connections=input_connections,
                    output_properties=output_properties, resources=resources)

        else:
            return self._normal_net(input_connections=input_connections,
                    resources=resources)

    @property
    def parallel_by(self):
        return self.xml.attrib.get('parallelBy')

    @property
    def execute_action_class(self):
        if self.local_workflow:
            return actions.ForkAction
        else:
            return actions.LSFAction

    @property
    def shortcut_action_class(self):
        return actions.ForkAction

    def _parallel_by_net(self, input_connections, output_properties, resources):
        target_net = self._normal_net(input_connections, resources)
        return future_nets.ParallelByNet(target_net, self.parallel_by,
                output_properties=output_properties)

    def _normal_net(self, input_connections, resources):
        return future_nets.PerlActionNet(
                name=self.name,
                operation_id=self.operation_id,
                parent_operation_id=self.parent.operation_id,
                input_connections=input_connections,
                resources=resources,
                stderr=self.log_manager.stderr_log_path,
                stdout=self.log_manager.stdout_log_path,
                action_type=self.action_type,
                action_id=self.action_id,
                shortcut_action_class=self.shortcut_action_class,
                execute_action_class=self.execute_action_class)
