from flow_workflow.entities.perl_action import adapter_base


class EventAdapter(adapter_base.PerlActionAdapterBase):
    action_type = 'event'

    @property
    def action_id(self):
        return self.event_id

    @property
    def event_id(self):
        return self.operation_type_attributes['eventId']

