# node of DAG
import copy

import six


class Node (object):

    def __init__(self, id, node_type, data, is_leaf, name):
        self.id = id
        self.node_type = node_type
        self.data = data
        self.is_leaf = is_leaf
        self.is_tail = False
        self.inputs = {}
        self.outputs = {}
        self.scatter = None
        self.parents = set()
        self.name = name
        self.sub_nodes = set()
        self.root_inputs = None

    def add_parent(self, id):
        self.parents.add(id)

    def set_input_value(self, key, src_key, src_value):
        if isinstance(self.inputs[key]['source'], list):
            self.inputs[key].setdefault('value', copy.copy(self.inputs[key]['source']))
            tmp_list = []
            for k in self.inputs[key]['value']:
                if k == src_key:
                    tmp_list.append(src_value)
                else:
                    tmp_list.append(k)
            self.inputs[key]['value'] = tmp_list
        else:
            self.inputs[key]['value'] = src_value

    # convert inputs to dict inputs
    def convert_dict_inputs(self):
        data = {}
        for k, v in six.iteritems(self.inputs):
            y_name = k.split('/')[-1]
            if 'value' in v:
                data[y_name] = v['value']
            elif 'default' in v:
                data[y_name] = v['default']
            else:
                raise ReferenceError("{} is not resolved".format(k))
        return data

    def __str__(self):
        outstr = "ID:{} Name:{} Type:{}\n".format(self.id, self.name, self.node_type)
        outstr += "  Parent:{}\n".format(','.join([str(p) for p in self.parents]))
        outstr += "  Input:\n"
        for k, v in six.iteritems(self.convert_dict_inputs()):
            outstr += "     {}: {}\n".format(k, v)
        outstr += "  Output:\n"
        for k, v in six.iteritems(self.outputs):
            if 'value' in v:
                v = v['value']
            else:
                v = 'NA'
            outstr += "     {}: {}\n".format(k, v)
        return outstr


# dump nodes
def dump_nodes(node_list, only_leaves=True):
    for node in node_list:
        if node.is_leaf:
            print(node)
        else:
            if not only_leaves:
                print(node)
            dump_nodes(node.sub_nodes)
