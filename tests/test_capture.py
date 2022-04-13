import logprov.capture
from logprov.io import read_prov, provlist2provdoc, provdoc2svg
import yaml
import datetime
from shutil import copyfile

provconfig = {
    'capture': True,
    'hash_type': 'sha1',
    'log_filename': 'prov_test.log',
    'log_args': False,
    'log_args_as_entities': False,
    'log_kwargs': False,
    'log_returned_result': False,
}

definitions_yaml = """
activity_descriptions:
    regular_function:
        description: "set initial value of global_var"
        parameters:
            - value: kwargs.value
        generation:
            - role: global_var
              entity_description: MyObject
              value: global_var
    set_var1:
        description: "set initial value of var1"
        parameters:
            - value: kwargs.value
        generation:
            - role: var1
              entity_description: MyObject
              value: var1
    set_var2:
        description: "set value of var2 using var1"
        usage:
            - role: var1
              entity_description: MyObject
              value: var1
            - role: global_var
              entity_description: MyObject
              value: global_var
            - role: local_var
              entity_description: MyObject
              value: local_var
        generation:
            - role: var2
              entity_description: MyObject
              value: var2
    write_file:
        description: "write var1 and var2 in a text file"
        parameters:
            - value: kwargs.filename
        usage:
            - role: var1
              entity_description: MyObject
              value: var1
            - role: var2
              entity_description: MyObject
              value: var2
        generation:
            - role: text file
              entity_description: File
              location: kwargs.filename
              namespace: "file"
    read_file:
        description: "read var1 and var2 from a text file"
        parameters:
            - value: kwargs.filename
        usage:
            - role: text file
              entity_description: File
              location: kwargs.filename
              namespace: "file"
        generation:
            - role: var1
              entity_description: MyObject
              value: var1
            - role: var2
              entity_description: MyObject
              value: var2
entity_descriptions:
    MyObject:
        description: "A Python variable in memory"
        type: PythonObject
    File:
        description: "A File on the disk"
        type: File
agents:
"""

definitions = yaml.safe_load(definitions_yaml)
# definitions = logprov.capture.definitions_default

prov_capture = logprov.ProvCapture(definitions=definitions, config=provconfig)
prov_capture.traced_variables = {}
prov_capture.logger.setLevel("DEBUG")


class MyObject(object):

    value = 0

    def __repr__(self):
        return str(self.value)


global_var = MyObject()
global_var.value = 100


@prov_capture.trace
def regular_function(value=100):
    print(f"regular_function(value={value})")
    global_var.value = value
    return global_var


@prov_capture.trace_methods
class Class1(object):

    def __init__(self):
        print(f"Class1.__init__()")
        self.var1 = MyObject()
        self.var2 = MyObject()

    def __repr__(self):
        return "Class1"

    def set_var1(self, value=0):
        self.var1.value = value
        print(f"set_var1(value={value})")
        return self.var1

    def set_var2(self, var1, gvar, add_to_value=0):
        local_var = MyObject()
        local_var.value = 10
        self.var2.value = self.var1.value + local_var.value + global_var.value + add_to_value
        print(f"set_var2({add_to_value})")
        return self.var2

    def untraced(self, value=0):
        self.var1.value = value
        print(f"untraced(value={value})")
        return self.var1

    def write_file(self, filename="prov_test.txt"):
        print(f"write_file(filename={filename})")
        with open(filename, "w") as f:
            f.write(f"A={self.var1} B={self.var2}")

    def read_file(self, filename="prov_test.txt"):
        print(f"read_file(filename={filename})")
        with open(filename, "r") as f:
            line = f.read()
        for item in line.split(" "):
            name, value = item.split("=")
            if name == "A":
                self.var1.value = int(value)
            if name == "B":
                self.var2.value = int(value)


start = datetime.datetime.now().isoformat()
#regular_function()
c1 = Class1()
c1.set_var1(value=1)
#c1.set_var1(value=2)
#c1.untraced(value=1)
#c1.var1.value = 5
#c1.set_var2(0)
#c1.set_var1()
#c1.set_var2(c1.var1, global_var, add_to_value=2)
#c1.write_file(filename="prov_test1.txt")
#copyfile("prov_test1.txt", "prov_test2.txt")
#c1.read_file(filename="prov_test2.txt")
#c1.set_var2(c1.var1, global_var, add_to_value=2)
end = datetime.datetime.now().isoformat()
print(start, "-->", end)

logname = provconfig['log_filename']
provlist = read_prov(logname=logname, start=start, end=end)
provdoc = provlist2provdoc(provlist)
# for pr in provdoc.get_records():
#     print(pr.get_provn())
provdoc.serialize(logname + '.json')
provdoc.serialize(logname + '.xml', format='xml')
provdoc2svg(provdoc, logname + '.svg')
