from flow.petri_net.actions.base import BasicActionBase

class WorkflowUpdateActionBase(BasicActionBase):
    required_args = ['operation_id']

    def execute(self, net, color_descriptor, active_tokens, service_interfaces):
        if env_is_perl_true(net, 'UR_DBI_NO_COMMIT'):
            LOG.debug('UR_DBI_NO_COMMIT is set, not updating status.')
            return [], defer.succeed(None)

        historian = service_interfaces['workflow_historian']

        workflow_data = io.extract_workflow_data(net, active_tokens)
        parallel_id = ParallelIdentifier(workflow_data.get('parallel_id', []))

        self._execute(historian=historian, net=net,
                color_descriptor=color_descriptor, parallel_id=parallel_id,
                workflow_data=workflow_data)

    @property
    def operation(self):
        return factory.load_operation(self.args['operation_id'])

def get_workflow_plan_id(net):
    return net.constant('workflow_plan_id')

def get_peer_fields(operation, parallel_id, color_descriptor):
    fields = {}
    if parallel_id.refers_to(operation):
        fields['peer_net_key'] = operation.net.key
        fields['peer_operation_id'] = operation.operation_id
        fields['peer_color'] = color_descriptor.group.begin
        fields['parallel_index'] = parallel_id.index
    return fields

def submit_operation(historian, net, operation, color_descriptor,
        parallel_id, workflow_data, status):
    fields = {
            'net_key': operation.net_key,
            'operation_id': operation.operation_id,
            'color': color_descriptor.color,
            'name': operation.name,
            'workflow_plan_id': get_workflow_plan_id(net),
            'parent_net_key': operation.parent.net_key,
            'parent_operation_id': operation.parent.operation_id,
            'parent_color': color_descriptor.group.parent_color,
            'status': 'new',
            }
    fields.update(get_peer_fields(operation, parallel_id, color_descriptor))
    return historian.update(**fields)

class UpdateModelAction(WorkflowUpdateActionBase):

    def _execute(historian, net, color_descriptor, parallel_id,
            workflow_data):
        deferreds = []
        for child_operation in self.operation.children:
            deferred = self._submit_operation(historian, net, operation_id,
                    color_descriptor, parallel_id, workflow_data)
            deferreds.append(deferred)
        return defer.gatherResults(deferreds)


def env_is_perl_true(net, varname):
    env = net.constant('environment')
    try:
        var = env.get(varname)
        return var_is_perl_true(var)
    except:
        pass

    return False


_PERL_FALSE_VALUES = set([
    '0',
    '',
])
def var_is_perl_true(var):
    return var and (str(var) not in _PERL_FALSE_VALUES)
