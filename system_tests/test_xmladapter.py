import flow_workflow.xmladapter as wfxml

import flow.petri.netbuilder as nb

from test_helpers import RedisTest
from lxml import etree
from lxml.builder import E
from unittest import main


WFXML = """<workflow name="test converge">
  <link toProperty="a" toOperation="A" fromOperation="input connector" fromProperty="a"/>
  <link toProperty="b" toOperation="B" fromOperation="input connector" fromProperty="b"/>
  <link toProperty="a" toOperation="C" fromOperation="A" fromProperty="a"/>
  <link toProperty="b" toOperation="C" fromOperation="B" fromProperty="b"/>
  <link toProperty="c" toOperation="output connector" fromOperation="C" fromProperty="c"/>
  <link toProperty="d" toOperation="output connector" fromOperation="C" fromProperty="d"/>
  <operationtype typeClass="Workflow::OperationType::Model"/>
  <operation name="A">
    <operationtype typeClass="Workflow::OperationType::Command" commandClass="ClassA"/>
  </operation>
  <operation name="B">
    <operationtype typeClass="Workflow::OperationType::Command" commandClass="ClassB"/>
  </operation>
  <operation name="C">
    <operationtype typeClass="Workflow::OperationType::Converge">
      <inputproperty>a</inputproperty>
      <inputproperty>b</inputproperty>
      <outputproperty>c</outputproperty>
      <outputproperty>d</outputproperty>
    </operationtype>
  </operation>
</workflow>"""


class TestStoreWorkflow(RedisTest):
    def setUp(self):
        RedisTest.setUp(self)
        self.builder = nb.NetBuilder()

    def test_build_and_store(self):
        xml = etree.XML(WFXML)
        model = wfxml.parse_workflow_xml(xml, resources={},
                net_builder=self.builder, plan_id=20)

        net = self.builder.store(self.conn)
        self.assertEqual(20, net.constant("workflow_plan_id"))

        # There are 6 operations:
        #  #0: the outer model
        #  #1: input connector
        #  #2: output connector
        #  #3-5: ops A, B, C
        # The next id should be 6
        self.assertEqual(6, net.constant("workflow_next_operation_id"))


if __name__ == "__main__":
    main()
