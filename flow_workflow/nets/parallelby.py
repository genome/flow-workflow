from flow_workflow.nets.core import InputsMixin, GenomeEmptyNet
from flow_workflow.nets.io import StoreOutputsAction
from flow_workflow.nets.perlaction import GenomePerlActionNet

import flow.petri.safenet as sn
import flow.petri.netbuilder as nb

from collections import namedtuple


ParallelBySpec = namedtuple("ParallelBySpec", "property index")


class BuildParallelByAction(InputsMixin, sn.TransitionAction):
    required_arguments = ["action_type", "action_id", "parallel_by",
            "input_connections", "operation_id", "success_place",
            "failure_place", "parent_operation_id"]

    def execute(self, active_tokens_key, net, service_interfaces):
        action_type = self.args["action_type"]
        action_id = self.args["action_id"]
        parallel_by = self.args["parallel_by"]

        inputs = self.input_data(active_tokens_key, net)

        builder = nb.NetBuilder()
        pby_net = builder.add_subnet(nb.EmptyNet, self.name)

        pby_net.start_place = pby_net.add_place("start")
        pby_net.success_place = pby_net.add_place("success")
        pby_net.failing_place = pby_net.add_place("failing")
        pby_net.failure_place = pby_net.add_place("failure")

        args = {"operation_id": 0}
        store_outputs_action = nb.ActionSpec(cls=StoreOutputsAction, args=args)
        pby_net.start_transition = pby_net.add_transition("start",
                action=store_outputs_action)

        pby_net.success_transition = pby_net.add_transition("success")
        pby_net.failure_transition = pby_net.add_transition("failure")

        pby_net.start_place.arcs_out.add(pby_net.start_transition)
        pby_net.failing_place.arcs_out.add(pby_net.failure_transition)
        pby_net.success_transition.arcs_out.add(pby_net.success_place)
        pby_net.failure_transition.arcs_out.add(pby_net.failure_place)

        input_conns = {0: {x: x for x in inputs}}
        input_source_id = 0

        for i, value in enumerate(inputs[parallel_by]):
            op_id = i+1
            name = "%s (#%d)" % (self.name, op_id)

            pby_spec = ParallelBySpec(property=parallel_by, index=i)

            subnet = pby_net.add_subnet(GenomePerlActionNet,
                    name=name,
                    operation_id=op_id,
                    parent_operation_id=self.args["parent_operation_id"],
                    input_connections=input_conns,
                    action_type=action_type,
                    action_id=action_id,
                    parallel_by_spec=pby_spec)

            done = pby_net.add_place("%d done" % i)
            subnet.success_transition.arcs_out.add(done)
            subnet.failure_transition.arcs_out.add(done)

            done.arcs_out.add(pby_net.success_transition)
            done.arcs_out.add(pby_net.failure_transition)

            pby_net.bridge_transitions(pby_net.start_transition,
                    subnet.start_transition)

            pby_net.bridge_transitions(subnet.success_transition,
                    pby_net.success_transition)

            subnet.failure_transition.arcs_out.add(pby_net.failing_place)

        success_args = {"remote_net_key": net.key,
                "remote_place_id": self.args["success_place"],
                "data_type": "output"}

        failure_args = {"remote_net_key": net.key,
                "remote_place_id": self.args["failure_place"],
                "data_type": "output"}

        pby_net.success_transition.action = nb.ActionSpec(
                cls=sn.SetRemoteTokenAction,
                args=success_args)

        pby_net.failure_transition.action = nb.ActionSpec(
                cls=sn.SetRemoteTokenAction,
                args=failure_args)

        stored_net = builder.store(self.connection)
        stored_net.copy_constants_from(net)

        orchestrator = service_interfaces["orchestrator"]
        token = sn.Token.create(self.connection, data=inputs, data_type="output")
        orchestrator.set_token(stored_net.key, pby_net.start_place.index, token.key)


class GenomeParallelByNet(GenomeEmptyNet):
    def __init__(self, builder, name, operation_id, parent_operation_id,
            input_connections, action_type, action_id, parallel_by, stdout=None,
            stderr=None, queue=None, resources=None):

        GenomeEmptyNet.__init__(self, builder, name, operation_id,
                parent_operation_id, input_connections, queue, resources)

        self.parallel_by = parallel_by
        self.action_type = action_type
        self.action_id = action_id

        self.running = self.add_place("running")
        self.on_success = self.add_place("on_success")
        self.on_failure = self.add_place("on_failure")

        args = {
            "action_type": self.action_type,
            "action_id": self.action_id,
            "with_outputs": True,
            "operation_id": self.operation_id,
            "input_connections": self.input_connections,
            "parallel_by": parallel_by,
            "parent_operation_id": parent_operation_id,
            "stdout": stdout,
            "stderr": stderr,
            "success_place": self.on_success.index,
            "failure_place": self.on_failure.index,
            "resources": self.resources
        }

        action = nb.ActionSpec(cls=BuildParallelByAction, args=args)
        self.start_transition = self.add_transition("start_transition",
                action=action)

        self.success_transition = self.add_transition("success_transition")
        self.failure_transition = self.add_transition("failure_transition")

        self.start_transition.arcs_out.add(self.running)
        self.running.arcs_out.add(self.success_transition)
        self.running.arcs_out.add(self.failure_transition)

        self.on_success.arcs_out.add(self.success_transition)
        self.on_failure.arcs_out.add(self.failure_transition)


