from flow_workflow.perl_action import adapter_base


class CommandAdapter(adapter_base.PerlActionAdapterBase):
    action_type = 'command'
    operation_class = action_type

    @property
    def action_id(self):
        return self.command_class

    @property
    def command_class(self):
        return self.operation_type_attributes['commandClass']
