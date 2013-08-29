from flow_workflow.commands.launch_base import LaunchWorkflowCommandBase
from flow.configuration.inject.broker import BrokerConfiguration
from flow.configuration.inject.redis_conf import RedisConfiguration
from flow.configuration.inject.service_locator import ServiceLocatorConfiguration
from twisted.internet import defer

import os


class SubmitWorkflowCommand(LaunchWorkflowCommandBase):
    injector_modules = [
            BrokerConfiguration,
            RedisConfiguration,
            ServiceLocatorConfiguration,
    ]

    local_workflow = False

    def setup_services(self, net):
        pass

    def wait_for_results(self, net, block):
        if block:
            return self.setup_completion_handler(net)
        else:
            return defer.succeed(block)

    @property
    def additional_constants(self):
        return { 'groups': os.getgroups() }
