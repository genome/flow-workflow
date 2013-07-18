from flow_workflow.perl_action import actions
from flow_workflow.perl_action import future_nets
from flow_workflow.log_manager import LogManager
from flow_workflow.parallel_by import adapter_base

import abc


class PerlActionAdapterBase(adapter_base.ParallelXMLAdapterBase):
    operation_class = 'direct_storage'

    def __init__(self, *args, **kwargs):
        adapter_base.ParallelXMLAdapterBase.__init__(self, *args, **kwargs)
        self.log_manager = LogManager(log_dir=self.log_dir,
                operation_id=self.operation_id,
                operation_name=self.name)

    @abc.abstractproperty
    def action_type(self):
        pass

    @abc.abstractproperty
    def action_id(self):
        pass

    @property
    def execute_action_class(self):
        if self.local_workflow:
            return actions.ForkAction
        else:
            return actions.LSFAction

    @property
    def shortcut_action_class(self):
        return actions.ForkAction

    def single_future_net(self, input_connections, output_properties,
            resources):
        return future_nets.PerlActionNet(
                name=self.name,
                operation_id=self.operation_id,
                resources=resources,
                stderr=self.log_manager.stderr_log_path,
                stdout=self.log_manager.stdout_log_path,
                action_type=self.action_type,
                action_id=self.action_id,
                shortcut_action_class=self.shortcut_action_class,
                execute_action_class=self.execute_action_class)
