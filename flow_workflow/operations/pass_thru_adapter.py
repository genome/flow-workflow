from flow_workflow.operations.adapter_base import AdapterBase

import logging


LOG = logging.getLogger(__name__)

class PassThruAdapter(AdapterBase):
    net_class = 'DEFINE IN SUBCLASSES'

    def get_net_class_kwargs(self, input_connections):
        return {
                'name':self.name,
                'self_id':self.self_id,
                'input_connections':input_connections,
                'parent_self_id':self.parent.self_id,
        }

    def net(self, super_net, input_connections=None, output_properties=None,
            resources=None):
        kwargs = self.get_net_class_kwargs(self, input_connections)
        return super_net.add_subnet(self.net_class, **kwargs)
