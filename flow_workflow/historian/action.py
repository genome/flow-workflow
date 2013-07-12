#from flow import petri
#from twisted.internet import defer
#
#import os
#import logging
#from time import localtime, strftime
#
#LOG = logging.getLogger(__name__)
#TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S"
#
#class WorkflowHistorianUpdateAction(petri.TransitionAction):
#    required_args = ['children_info']
#    optional_args = ['token_data_map', 'timestamps', 'shortcut',
#            'net_constants_map']
#
#    def _set_peer_info(self, net, child_info):
#        peer_operation_id = child_info.get('peer_operation_id')
#        if peer_operation_id is not None:
#            child_info['peer_net_key'] = net.key
#
#    def _set_parent_info(self, net, child_info):
#        parent_operation_id = child_info.get('parent_operation_id')
#        if parent_operation_id is not None:
#            parent_net_key = child_info.get("parent_net_key")
#            if parent_net_key is None:
#                child_info['parent_net_key'] = net.key
#        else: # This is a "top level" workflow
#            parent_net_key = net.constant("workflow_parent_net_key")
#            parent_operation_id = net.constant("workflow_parent_operation_id")
#
#            if parent_net_key is not None and parent_operation_id is not None:
#                child_info['parent_net_key'] = parent_net_key
#                child_info['parent_operation_id'] = parent_operation_id
#                child_info['is_subflow'] = True
#            else:
#                LOG.info("Unable to determine parent for action %r",
#                        self.args.value)
#
#    def _timestamp(self):
#        now = self.connection.time()
#        # convert (sec, microsec) from redis to floating point sec
#        now = now[0] + now[1] * 1e-6
#
#        return strftime(TIMESTAMP_FORMAT, localtime(now)).upper()
#
#    def _get_net_attributes(self, net):
#        rv = {}
#        name_mapping = self.args.get('net_attributes_map', {})
#        for net_name, historian_name in name_mapping.iteritems():
#            value = getattr(net, net_name, None)
#            if value:
#                rv[historian_name] = value
#        return rv
#
#    def _get_net_constants(self, net):
#        rv = {}
#        name_mapping = self.args.get('net_constants_map', {})
#        for net_name, historian_name in name_mapping.iteritems():
#            value = net.constant(net_name)
#            if value:
#                rv[historian_name] = value
#
#        return rv
#
#    def _get_token_data(self, active_tokens_key):
#        rv = {}
#        token_data_map = self.args.get('token_data_map')
#        if token_data_map:
#            tokens = self.tokens(active_tokens_key)
#            token_data = petri.merge_token_data(tokens)
#            for token_name, historian_name in token_data_map.iteritems():
#                rv[historian_name] = token_data[token_name]
#
#        shortcut = self.args.get('shortcut')
#        if shortcut is True and 'dispatch_id' in rv:
#            rv['dispatch_id'] = 'P%s' % rv['dispatch_id']
#
#        return rv
#
#    def _get_runtime_fields(self, active_tokens_key, net):
#        fields = {}
#        timestamps = self.args.get('timestamps', [])
#        fields.update({t: self._timestamp() for t in timestamps})
#
#        fields.update(self._get_token_data(active_tokens_key))
#        fields.update(self._get_net_constants(net))
#        fields.update(self._get_net_attributes(net))
#
#        return fields
#
#    def execute(self, active_tokens_key, net, service_interfaces):
#        if env_is_perl_true(net, 'UR_DBI_NO_COMMIT'):
#            LOG.debug('UR_DBI_NO_COMMIT is set, not updating status.')
#            return defer.succeed(None)
#
#
#        historian = service_interfaces['workflow_historian']
#        net_key = net.key
#
#        fields = self._get_runtime_fields(active_tokens_key, net)
#
#        deferreds = []
#        for child_info in self.args['children_info']:
#            child_info.update(fields)
#
#            operation_id = child_info.pop('id', None)
#            if operation_id is None:
#                raise RuntimeError("Null operation id in historian update: %r" %
#                        self.args.value)
#
#            child_info['workflow_plan_id'] = net.constant("workflow_plan_id")
#            self._set_parent_info(net, child_info)
#            self._set_peer_info(net, child_info)
#
#            parent = os.environ.get("FLOW_WORKFLOW_PARENT_ID")
#            LOG.debug("Historian update: (operation=%r, parent=%s), %r",
#            operation_id, parent, child_info)
#
#            deferred = historian.update(net_key=net_key,
#                        operation_id=operation_id, **child_info)
#            deferreds.append(deferred)
#
#        dlist = defer.gatherResults(deferreds)
#        execute_deferred = defer.Deferred()
#        dlist.addCallback(lambda _: execute_deferred.callback(None))
#        return execute_deferred
#
#def env_is_perl_true(net, varname):
#    env = net.constant('environment')
#    try:
#        var = env.get(varname)
#        return var_is_perl_true(var)
#    except:
#        pass
#
#    return False
#
#_PERL_FALSE_VALUES = set([
#    '0',
#    '',
#])
#def var_is_perl_true(var):
#    return var and (str(var) not in _PERL_FALSE_VALUES)
