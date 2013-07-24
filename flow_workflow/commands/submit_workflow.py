from flow_workflow.commands.launch_base import LaunchWorkflowCommandBase
from flow.configuration.inject.broker import BrokerConfiguration
from flow.configuration.inject.redis_conf import RedisConfiguration
from flow.configuration.inject.service_locator import ServiceLocatorConfiguration
from twisted.internet import defer


class SubmitWorkflowCommand(LaunchWorkflowCommandBase):
    injector_modules = [
            BrokerConfiguration,
            RedisConfiguration,
            ServiceLocatorConfiguration,
    ]

    local_workflow = True

    def setup_services(self, net):
        pass

    def wait_for_results(self, block):
        return defer.succeed(block)
