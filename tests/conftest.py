import pytest

from flowpipe.node import INode, Node
from flowpipe.plug import InputPlug, OutputPlug
from flowpipe.graph import Graph

class NodeForTesting(INode):
    """
    +---------------------+
    |   NodeForTesting    |
    |---------------------|
    o in1<>               |
    o in2<>               |
    |                 out o
    |                out2 o
    +---------------------+
    """

    def __init__(self, name=None, in1=None, in2=None, **kwargs):
        super(NodeForTesting, self).__init__(name, **kwargs)
        OutputPlug('out', self)
        OutputPlug('out2', self)
        InputPlug('in1', self, in1)
        InputPlug('in2', self, in2)

    def compute(self, in1, in2):
        """Multiply the two inputs."""
        return {'out': in1 * in2, 'out2': None}

@pytest.fixture
def branching_graph():
    """
    +------------+          +------------+          +--------------------+
    |   Start    |          |   Node2    |          |        End         |
    |------------|          |------------|          |--------------------|
    o in1<0>     |     +--->o in1<>      |          % in1                |
    o in2<0>     |     |    o in2<0>     |     +--->o  in1.1<>           |
    |        out o-----+    |        out o-----|--->o  in1.2<>           |
    |       out2 o     |    |       out2 o     |    o in2<0>             |
    +------------+     |    +------------+     |    |                out o
                       |    +------------+     |    |               out2 o
                       |    |   Node1    |     |    +--------------------+
                       |    |------------|     |
                       +--->o in1<>      |     |
                            o in2<0>     |     |
                            |        out o-----+
                            |       out2 o
                            +------------+
    """
    graph = Graph(name="TestGraph")
    start = NodeForTesting(name='Start', graph=graph)
    n1 = NodeForTesting(name='Node1', graph=graph)
    n2 = NodeForTesting(name='Node2', graph=graph)
    end = NodeForTesting(name='End', graph=graph)
    start.outputs['out'] >> n1.inputs['in1']
    start.outputs['out'] >> n2.inputs['in1']
    n1.outputs['out'] >> end.inputs['in1']['1']
    n2.outputs['out'] >> end.inputs['in1']['2']

    yield graph


@pytest.fixture
def nested_graph():
    """
    +----main----+          +----sub1----+                  +--------sub2--------+
    |   Start    |          |   Node1    |                  |        End         |
    |------------|          |------------|                  |--------------------|
    o in1<>      |     +--->o in1<>      |                  % in1                |
    o in2<>      |     |    o in2<>      |         +------->o  in1.1<>           |
    |        out o-----+    |        out o---------+   +--->o  in1.2<>           |
    |       out2 o     |    |       out2 o             |--->o in2<>              |
    +------------+     |    +------------+             |    |                out o
                       |    +--------sub1--------+     |    |               out2 o
                       |    |       Node2        |     |    +--------------------+
                       |    |--------------------|     |                          
                       |    % in1                |     |                          
                       +--->o  in1.0<>           |     |                          
                            o in2<>              |     |                          
                            |                out %     |                          
                            |             out.0  o-----+                          
                            |               out2 o                                
                            +--------------------+                                
    """
    main = Graph(name="main")
    sub1 = Graph(name="sub1")
    sub2 = Graph(name="sub2")

    start = NodeForTesting(name='Start', graph=main)
    n1 = NodeForTesting(name='Node1', graph=sub1)
    n2 = NodeForTesting(name='Node2', graph=sub1)
    end = NodeForTesting(name='End', graph=sub2)
    start.outputs['out'] >> n1.inputs['in1']
    start.outputs['out'] >> n2.inputs['in1']['0']
    n1.outputs['out'] >> end.inputs['in1']['1']
    n2.outputs['out']['0'] >> end.inputs['in1']['2']
    n2.outputs['out']['0'] >> end.inputs['in2']

    yield main