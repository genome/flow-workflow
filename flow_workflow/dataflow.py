class DataArc(object):
    def __init__(self, src, src_prop, dst, dst_prop):
        self.src = src
        self.src_prop = src_prop
        self.dst = dst
        self.dst_prop = dst_prop

class DataArcs(object):
    def __init__(self):
        self.arcs_out = {}
        self.arcs_in = {}

    def add(self, arc):
        self.arcs_in.setdefault(arc.dst, []).append(arc)
        self.arcs_out.setdefault(arc.src, []).append(arc)

    def to_output_hash(self):
        """Returns a hash: h[src][dst][src_prop] = dst_prop"""

        rv = {}
        for src, dst in self.arcs_out.iteritems():
            for arc in dst:
                src_hash = rv.setdefault(arc.src, {})
                src_hash.setdefault(arc.dst, {})[arc.src_prop] = arc.dst_prop

        return rv

    def to_input_hash(self):
        """Returns a hash: h[dst][src][dst_prop] = src_prop"""

        rv = {}
        for src, dst in self.arcs_out.iteritems():
            for arc in dst:
                dst_hash = rv.setdefault(arc.dst, {})
                dst_hash.setdefault(arc.src, {})[arc.dst_prop] = arc.src_prop

        return rv

