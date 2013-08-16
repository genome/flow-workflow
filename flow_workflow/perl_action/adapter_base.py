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

    @property
    def project_name(self):
        return self.operation_type_attributes.get('lsfProject')

    def single_future_net(self, resources):
        return future_nets.PerlActionNet(
                name=self.name,
                operation_id=self.operation_id,
                resources=resources,
                action_type=self.action_type,
                action_id=self.action_id,
                project_name=self.project_name,
                shortcut_action_class=self.shortcut_action_class,
                execute_action_class=self.execute_action_class)
