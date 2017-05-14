"""Nodes manipulate incoming data and provide the outgoing data."""
from __future__ import print_function
from abc import ABCMeta, abstractmethod
import uuid

from flowpipe.log_observer import LogObserver
__all__ = ['INode']


class INode(object):
    """Holds input and output Plugs and a method for computing."""

    __metaclass__ = ABCMeta

    def __init__(self, name=None, identifier=None):
        """Initialize the input and output dictionaries and the name.

        Args:
            name (str): If not provided, the class name is used.
        """
        self.name = name if name is not None else self.__class__.__name__
        self.identifier = (identifier if identifier is not None
                           else '{0}-{1}'.format(self.name, uuid.uuid4()))
        self.inputs = dict()
        self.outputs = dict()

    def __unicode__(self):
        """Show all input and output Plugs."""
        offset = ''
        if [i for i in self.inputs.values() if i.connections]:
            offset = ' '*3
        width = len(max(self.inputs.keys() + self.outputs.keys() + [self.name], key=len)) + 2
        pretty = offset + '+' + '-'*width + '+'
        pretty += '\n{offset}|{name:/^{width}}|'.format(offset=offset, name=' ' + self.name + ' ', width=width)
        pretty += '\n' + offset + '|' + '-'*width + '|'
        # Inputs
        for i, input_ in enumerate(self.inputs.keys()):
            pretty += '\n'
            if self.inputs[input_].connections:
                pretty += '-->'
            else:
                pretty += offset
            pretty += 'o {input_:{width}}|'.format(input_=input_, width=width-1)

        # Outputs
        for i, output in enumerate(self.outputs.keys()):
            pretty += '\n{offset}|{output:>{width}} o'.format(offset=offset, output=output, width=width-1)
            if self.outputs[output].connections:
                pretty += '-->'

        pretty += '\n' + offset + '+' + '-'*width + '+'

        return pretty

    def __str__(self):
        """Show all input and output Plugs."""
        return self.__unicode__().encode('utf-8').decode()

    @property
    def is_dirty(self):
        """Whether any of the input Plug data has changed and is dirty."""
        for input_ in self.inputs.values():
            if input_.is_dirty:
                return True
        return False

    @property
    def upstream_nodes(self):
        """The upper level Nodes that feed inputs into this Node."""
        upstream_nodes = list()
        for input_ in self.inputs.values():
            upstream_nodes += [c.node for c in input_.connections]
        return list(set(upstream_nodes))

    @property
    def downstream_nodes(self):
        """The next level Nodes that this Node feed outputs into."""
        downstream_nodes = list()
        for output in self.outputs.values():
            downstream_nodes += [c.node for c in output.connections]
        return list(set(downstream_nodes))

    def evaluate(self):
        """Compute this Node, log it and clean the input Plugs."""
        inputs = {name: plug.value for name, plug in self.inputs.items()}

        # Compute and redirect the output to the output plugs
        outputs = self.compute(**inputs) or dict()
        for name, value in outputs.items():
            self.outputs[name].value = value

        # Set the inputs clean
        for input_ in self.inputs.values():
            input_.is_dirty = False

        LogObserver.push_message('Computed: {}'.format(self.name))

        return outputs

    @abstractmethod
    def compute(self, **args):
        """Implement the data manipulation in the subclass.

        Return a dictionary with the outputs from this function.
        """
        pass

    def on_input_plug_set_dirty(self, input_plug):
        """Propagate the dirty state to the connected downstream nodes.

        Args:
            input_plug (IPlug): The Plug that got set dirty.
        """
        for output_plug in self.outputs.values():
            for connected_plug in output_plug.connections:
                connected_plug.is_dirty = True

    def serialize(self):
        """Serialize the node to json."""
        return {
            'module': self.__module__,
            'cls': self.__class__.__name__,
            'name': self.name,
            'identifier': self.identifier,
            'inputs': [plug.serialize() for plug in self.inputs.values()],
            'outputs': [plug.serialize() for plug in self.outputs.values()]
        }

    def deserialize(self, data):
        """De-serialize from the given json data."""
        self.name = data['name']
        self.identifier = data['identifier']

        for input_ in data['inputs']:
            self.inputs[input_['name']].value = input_['value']
