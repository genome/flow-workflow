from flow_workflow.perl_action import adapter_base


class EventAdapter(adapter_base.PerlActionAdapterBase):
    action_type = 'event'
    operation_class = action_type

    @property
    def action_id(self):
        return self.event_id

    @property
    def event_id(self):
        return self.operation_type_attributes['eventId']

