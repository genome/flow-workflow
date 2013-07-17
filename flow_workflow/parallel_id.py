from collections import OrderedDict

import json
import logging


LOG = logging.getLogger(__name__)


class ParallelIdentifier(object):
    def __init__(self, parallel_id=[]):
        self._entries = OrderedDict([(int(op_id), int(par_idx))
                for op_id, par_idx in parallel_id])

    @property
    def _parent_entries(self):
        parent_entries = OrderedDict(self._entries)
        parent_entries.popitem()
        return parent_entries

    @property
    def parent_identifier(self):
        return ParallelIdentifier(self._parent_entries.iteritems())

    def _child_entries(self, operation_id, parallel_idx):
        if int(operation_id) in self._entries:
            raise ValueError('operation_id already in ParallelIdentifier '
                    'op_id (%r) in %r' % (operation_id, self._entries))

        child_entries = OrderedDict(self._entries)
        child_entries[int(operation_id)] = int(parallel_idx)
        return child_entries

    def child_identifier(self, operation_id, parallel_idx):
        return ParallelIdentifier(self._child_entries(
            operation_id, parallel_idx).iteritems())

    @property
    def stack_iterator(self):
        current_id = self
        while len(current_id):
            yield current_id
            current_id = current_id.parent_identifier
        yield current_id

    def __iter__(self):
        return self._entries.iteritems()

    def __len__(self):
        return len(self._entries)

    def __repr__(self):
        return 'ParallelIdentifier(%r)' % list(self)

    def __cmp__(self, other):
        return cmp(self._entries, other._entries)

    def serialize(self):
        return json.dumps(list(self))

    @classmethod
    def deserialize(cls, data='[]'):
        return cls(json.loads(data))
