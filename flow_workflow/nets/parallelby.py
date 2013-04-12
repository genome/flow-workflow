from flow_workflow.nets.core import InputsMixin, GenomeEmptyNet
from flow_workflow.nets.perlaction import GenomePerlActionNet

from flow_workflow.nets.io import StoreOutputsAction
from flow_workflow.nets.io import operation_outputs
from flow import petri
import flow.petri.netbuilder as nb

from collections import namedtuple

import logging

LOG = logging.getLogger(__name__)


ParallelBySpec = namedtuple("ParallelBySpec",
        "property index parent_net_key peer_operation_id")


def add_parallel_subnets(name_base, net, num_subnets, action_type, action_id,
        input_names, parallel_by, parent_net_key,
        parent_operation_id, stdout_base, stderr_base):

    input_source_id = 0
    input_conns = {input_source_id: {x: x for x in input_names}}

    op_ids = []
    op_id = 1
    i = 0
    op_ids.append(op_id)
    name = str(name_base)

    pby_spec = ParallelBySpec(property=parallel_by, index=i,
            parent_net_key=parent_net_key, peer_operation_id=1)

    stdout_log_file = "%s_%d" % (stdout_base, i)
    stderr_log_file = "%s_%d" % (stderr_base, i)

    subnet = net.add_subnet(GenomePerlActionNet,
            name=name,
            operation_id=op_id,
            parent_operation_id=parent_operation_id,
            input_connections=input_conns,
            action_type=action_type,
            action_id=action_id,
            stdout=stdout_log_file,
            stderr=stderr_log_file,
            parallel_by_spec=pby_spec)

    done = net.add_place("%d done" % i)
    subnet.success_transition.arcs_out.add(done)
    subnet.failure_transition.arcs_out.add(done)

    done.arcs_out.add(net.success_transition)
    done.arcs_out.add(net.failure_transition)

    net.bridge_transitions(net.start_transition,
            subnet.start_transition)

    net.bridge_transitions(subnet.success_transition,
            net.success_transition)

    subnet.failure_transition.arcs_out.add(net.failing_place)

    return op_ids


class ParallelByJoinAction(petri.ColorJoinAction):
    required_arguments = (petri.ColorJoinAction.required_arguments +
            ["remote_net_key", "remote_place_id", "data_type", "token_color"])

    def input_data(self, active_tokens_key, net):
        operation_id = 1
        num_tokens = net.num_token_colors.value
        all_outputs = [operation_outputs(net, operation_id, x)
                for x in xrange(num_tokens)]

        names = set()
        for x in all_outputs:
            names.update(x.keys())

        outputs = {}
        for name in names:
            outputs[name] = [x.get(name) for x in all_outputs]

        LOG.debug("ParallelBySuccessAction: ops: %r, all outputs: %r, "
                "num tokens: %r, outputs: %r",
                operation_id, all_outputs, num_tokens, outputs)

        return {"outputs": outputs}

    def on_complete(self, active_tokens_key, net, service_interfaces):
        remote_net_key = self.args["remote_net_key"]
        remote_place_id = int(self.args["remote_place_id"])
        data_type = self.args["data_type"]
        token_color = self.args["token_color"]
        if token_color is not None:
            token_color = int(token_color)

        LOG.debug("Parallel by join completing, notifying %s place %d",
                remote_net_key, remote_place_id)
        input_data = self.input_data(active_tokens_key, net)
        token = petri.Token.create(self.connection, data=input_data,
                data_type=data_type, color_idx=token_color)

        orchestrator = service_interfaces['orchestrator']
        orchestrator.set_token(remote_net_key, remote_place_id, token.key)



class ParallelByNet(nb.EmptyNet):
    def __init__(self, builder, name, parallel_by, input_data, success_place,
            failure_place, action_type, action_id, parent_net_key,
            parent_operation_id, stderr_base, stdout_base):

        nb.EmptyNet.__init__(self, builder, name)

        self.start_place = self.add_place("start")
        self.success_place = self.add_place("success")
        self.failing_place = self.add_place("failing")
        self.failure_place = self.add_place("failure")

        store_outputs_args = {"operation_id": 0}
        store_outputs_action = nb.ActionSpec(cls=StoreOutputsAction,
                args=store_outputs_args)
        self.start_transition = self.add_transition("start",
                action=store_outputs_action)

        self.success_transition = self.add_transition("success")

        failure_args = {"remote_net_key": parent_net_key,
                "remote_place_id": failure_place,
                "data_type": "output"}
        failure_action = nb.ActionSpec(cls=petri.SetRemoteTokenAction,
                args=failure_args)
        self.failure_transition = self.add_transition("failure",
                action=failure_action)

        self.start_place.arcs_out.add(self.start_transition)
        self.failing_place.arcs_out.add(self.failure_transition)
        self.failure_transition.arcs_out.add(self.failure_place)

        num_subnets = len(input_data[parallel_by])
        input_names = input_data.keys()

        operation_id = add_parallel_subnets(
                name_base=self.name,
                net=self,
                num_subnets=num_subnets,
                action_type=action_type,
                action_id=action_id,
                input_names=input_names,
                parallel_by=parallel_by,
                parent_net_key=parent_net_key,
                parent_operation_id=parent_operation_id,
                stdout_base=stdout_base,
                stderr_base=stderr_base)

        join_args = {"remote_net_key": parent_net_key,
                "remote_place_id": success_place,
                "data_type": "output",
                "token_color": None}

        join_action = nb.ActionSpec(ParallelByJoinAction, args=join_args)
        self.join_transition = self.add_transition("join", action=join_action)
        self.bridge_transitions(self.success_transition, self.join_transition)
        self.join_transition.arcs_out.add(self.success_place)


class BuildParallelByAction(InputsMixin, petri.TransitionAction):
    required_arguments = ["action_type", "action_id", "parallel_by",
            "input_connections", "operation_id", "success_place",
            "failure_place", "parent_operation_id",
            "stdout_base", "stderr_base"]

    def execute(self, active_tokens_key, net, service_interfaces):
        inputs = self.input_data(active_tokens_key, net)
        parallel_by = self.args['parallel_by']
        num_operations = len(inputs[parallel_by])

        builder = nb.NetBuilder(net_type=petri.Net)

        parallel_net = builder.add_subnet(ParallelByNet,
                name=self.name,
                parallel_by=self.args['parallel_by'],
                input_data=inputs,
                parent_net_key=net.key,
                parent_operation_id=self.args['parent_operation_id'],
                success_place=self.args['success_place'],
                failure_place=self.args['failure_place'],
                action_type=self.args['action_type'],
                action_id=self.args['action_id'],
                stderr_base=self.args['stderr_base'],
                stdout_base=self.args['stdout_base'])

        stored_net = builder.store(self.connection)
        stored_net.set_num_token_colors(num_operations)
        stored_net.copy_constants_from(net)

        orchestrator = service_interfaces["orchestrator"]
        for i in xrange(num_operations):
            data = dict(inputs)
            data[parallel_by] = data[parallel_by][i]
            token = petri.Token.create(self.connection, data={"outputs": data},
                    data_type="output", color_idx=i)
            LOG.debug("Setting parallel by (#%d) token %s: data=%r",
                    i, token.key, data)
            deferred = orchestrator.set_token(stored_net.key,
                    parallel_net.start_place.index, token.key, token_color=i))
            deferreds.append(deferred)

        return None, defer.DeferredList(deferreds)


class GenomeParallelByNet(GenomeEmptyNet):
    def __init__(self, builder, name, child_base_name, operation_id,
            parent_operation_id, input_connections, action_type, action_id,
            parallel_by, stdout=None, stderr=None, queue=None, resources=None):

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
            "parent_operation_id": self.parent_operation_id,
            "stdout_base": stdout,
            "stderr_base": stderr,
            "success_place": self.on_success.index,
            "failure_place": self.on_failure.index,
            "resources": self.resources,
        }

        action = nb.ActionSpec(cls=BuildParallelByAction, args=args)
        self.start_transition = self.add_transition(child_base_name,
                action=action)

        store_outputs_action = nb.ActionSpec(
                cls=StoreOutputsAction,
                args={"operation_id": operation_id})

        self.success_transition = self.add_transition("success_transition",
                action=store_outputs_action)

        self.failure_transition = self.add_transition("failure_transition")

        self.start_transition.arcs_out.add(self.running)
        self.running.arcs_out.add(self.success_transition)
        self.running.arcs_out.add(self.failure_transition)

        self.on_success.arcs_out.add(self.success_transition)
        self.on_failure.arcs_out.add(self.failure_transition)
