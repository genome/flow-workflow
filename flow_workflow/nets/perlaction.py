from flow_workflow.nets.core import InputsMixin, GenomeEmptyNet
from flow_workflow.nets.io import StoreOutputsAction

import flow.command_runner.executors.nets as enets
import flow.petri.netbuilder as nb
import flow.petri.safenet as sn
import os

from collections import namedtuple

import logging

LOG = logging.getLogger(__name__)

GENOME_WRAPPER = "workflow-wrapper"


class GenomePerlAction(InputsMixin):
    output_token_type = 'input'
    required_arguments = ['operation_id', 'action_type', 'action_id', 'method']

    def _update_environment(self, net, env):
        parent_id = '%s %s' % (net.key, self.args['operation_id'])
        env['FLOW_WORKFLOW_PARENT_ID'] = parent_id

        LOG.debug('Setting environment variable FLOW_WORKFLOW_PARENT_ID=%s',
                parent_id)

        return env

    def _command_line(self, net, input_data_key):
        return [GENOME_WRAPPER, self.args["action_type"], self.args['method'],
                self.args["action_id"]]


class GenomeShortcutAction(GenomePerlAction, enets.LocalDispatchAction):
    def _environment(self, net):
        env = enets.LocalDispatchAction._environment(self, net)
        return self._update_environment(net, env)


class GenomeExecuteAction(GenomePerlAction, enets.LSFDispatchAction):
    def _environment(self, net):
        env = enets.LSFDispatchAction._environment(self, net)
        return self._update_environment(net, env)


class GenomeNet(GenomeEmptyNet):
    def __init__(self, builder, name, operation_id, parent_operation_id,
            input_connections, queue=None, resources=None):

        GenomeEmptyNet.__init__(self, builder, name, operation_id,
                parent_operation_id, input_connections, queue, resources)

        self.start_transition = self.add_transition("%s start_trans" % name)
        self.success_transition = self.add_transition("%s success_trans" % name)
        self.failure_transition = self.add_transition("%s failure_trans" % name)

        self.failure_place = self.add_place("%s failure" % name)
        self.failure_place.arcs_out.add(self.failure_transition)


class GenomeModelNet(GenomeNet):
    def __init__(self, builder, name, operation_id, parent_operation_id,
            input_connections, queue=None, resources=None):

        GenomeNet.__init__(self, builder, name, operation_id,
                parent_operation_id, input_connections, queue, resources)


        self.start_transition.action = nb.ActionSpec(
                cls=sn.MergeTokensAction,
                args={"input_type": "output", "output_type": "output"},
                )

        self.notify_start = self.add_transition("notify start",
                action=self._update_action(
                        status="running", timestamps=['start_time']))

        self.notify_done = self.add_transition("notify done",
                action=self._update_action(
                        status="done", timestamps=['end_time']))

        self.notify_failed = self.add_transition("notify start",
                action=self._update_action(
                        status="failed", timestamps=['end_time']))

        self.bridge_transitions(self.start_transition, self.notify_start)
        self.bridge_transitions(self.success_transition, self.notify_done)
        self.bridge_transitions(self.failure_transition, self.notify_failed)

        self.success_transition.action = self._update_action(status="done",
                timestamps=['end_time'])
        self.failure_transition.action = self._update_action(status="failed",
                timestamps=['end_time'])


class GenomePerlActionNet(GenomeNet):
    def __init__(self, builder, name, operation_id, parent_operation_id,
            input_connections, action_type, action_id, parallel_by_spec=None,
            stdout=None, stderr=None, queue=None, resources=None):

        GenomeNet.__init__(self, builder, name, operation_id, parent_operation_id,
                input_connections, queue, resources)

        self.action_type = action_type
        self.action_id = action_id

        base_args = {
                "name": self.name,
                "action_type": self.action_type, # command or event
                "action_id": self.action_id,     # command class or event id
                "with_outputs": True,
                "operation_id": self.operation_id,
                "input_connections": self.input_connections,
                "stdout": stdout,
                "stderr": stderr,
                }

        if parallel_by_spec:
            base_args["parallel_by"] = parallel_by_spec.property
            base_args["parallel_by_idx"] = parallel_by_spec.index
            # Note: self.parallel_index is only for the historian.
            # actual parallel by commands need to start at #1 in the historian.
            self.parallel_index = parallel_by_spec.index + 1
            self.parent_net_key = parallel_by_spec.parent_net_key
            self.peer_operation_id = parallel_by_spec.peer_operation_id

        shortcut_args = dict(base_args)
        execute_args = dict(base_args)

        shortcut_args['method'] = 'shortcut'

        execute_args.update({
                'method': 'execute',
                'resources': self.resources,
                'queue': self.queue,
                })

        store_outputs_action = nb.ActionSpec(cls=StoreOutputsAction,
                args={"operation_id": operation_id})

        self.shortcut = self.add_subnet(
                enets.LocalCommandNet, "%s shortcut" % name,
                action_class=GenomeShortcutAction, action_args=shortcut_args,
                begin_execute_action=self._update_action(shortcut=True,
                    token_data_map={"pid": "dispatch_id"},
                    timestamps=['start_time']
                    ),
                success_action=store_outputs_action,
                failure_action=None)

        self.execute = self.add_subnet(
                enets.LSFCommandNet, "%s execute" % name,
                action_class=GenomeExecuteAction, action_args=execute_args,
                dispatch_success_action=self._update_action(status="scheduled",
                        token_data_map={"pid": "dispatch_id"}),
                dispatch_failure_action=self._update_action(status="failed"),
                begin_execute_action=self._update_action(status="running",
                        timestamps=['start_time']),
                success_action=store_outputs_action,
                failure_action=None)

        self.start_transition.arcs_out.add(self.shortcut.start)

        self.success_place = self.add_place("%s success" % name)
        self.success_place.arcs_out.add(self.success_transition)

        self.bridge_places(self.shortcut.success, self.success_place, name="",
                action=self._update_action(
                    status="done",
                    timestamps=['end_time']))
        self.bridge_places(self.shortcut.failure, self.execute.start, name="")

        self.bridge_places(self.execute.success, self.success_place, name="",
                action=self._update_action(
                    status="done",
                    timestamps=['end_time']))
        self.bridge_places(self.execute.failure, self.failure_place, name="",
                action=self._update_action(
                    status="failed",
                    timestamps=['end_time']))
