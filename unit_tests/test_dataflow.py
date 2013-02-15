from flow_workflow.dataflow import DataArc, DataArcs

import unittest


class TestDataArcs(unittest.TestCase):
    def test_arcs(self):
        objs = [object() for x in xrange(4)]

        arcs = DataArcs()
        _arcs = [
            DataArc(src=objs[0], src_prop="a", dst=objs[1], dst_prop="w"),
            DataArc(src=objs[2], src_prop="b", dst=objs[1], dst_prop="x"),
            DataArc(src=objs[2], src_prop="c", dst=objs[1], dst_prop="y"),
            DataArc(src=objs[1], src_prop="d", dst=objs[3], dst_prop="z"),
        ]

        for arc in _arcs:
            arcs.add(arc)

        expected_output_hash = {
                objs[0]: {objs[1]: {"a": "w"}},
                objs[2]: {objs[1]: {"b": "x", "c": "y"}},
                objs[1]: {objs[3]: {"d": "z"}},
                }

        self.assertEqual(expected_output_hash, arcs.to_output_hash())

        expected_input_hash = {
                objs[1]: {objs[0]: {"w": "a"}, objs[2]: {"x": "b", "y": "c"}},
                objs[3]: {objs[1]: {"z": "d"}},
                }

        self.assertEqual(expected_input_hash, arcs.to_input_hash())


if __name__ == "__main__":
    unittest.main()
