from collections import OrderedDict


class ParallelIdentifier(object):
    def __init__(self, parallel_id=[]):
        self._entries = OrderedDict(parallel_id)

    @property
    def _parent_entries(self):
        parent_entries = OrderedDict(self._entries)
        parent_entries.popitem()
        return parent_entries

    @property
    def parent_identifier(self):
        return ParallelIdentifier(self._parent_entries)

    def _child_entries(self, operation_id, parallel_idx):
        assert operation_id not in self._entries

        child_entries = OrderedDict(self._entries)
        child_entries[operation_id] = parallel_idx
        return child_entries

    def child_identifier(self, operation_id, parallel_idx):
        return ParallelIdentifier(self._child_entries(
            operation_id, parallel_idx))

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
