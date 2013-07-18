from flow_workflow.parallel_by import future_nets

import abc
import flow_workflow.adapter_base


class ParallelXMLAdapterBase(flow_workflow.adapter_base.XMLAdapterBase):
    @abc.abstractmethod
    def single_future_net(self, resources):
        raise NotImplementedError()

    def future_net(self, resources):
        if self.parallel_by:
            return self._parallel_by_net(resources=resources)
        else:
            return self.single_future_net(resources=resources)

    @property
    def parallel_by(self):
        return self.xml.attrib.get('parallelBy')

    def _parallel_by_net(self, resources):
        target_net = self.single_future_net(resources=resources)
        return future_nets.ParallelByNet(target_net, self.parallel_by)
