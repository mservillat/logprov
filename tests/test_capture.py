import logprov.capture
from logprov.io import read_prov, provlist2provdoc, provdoc2svg
import yaml
import datetime

provconfig = {
    'capture': True,
    'hash_type': 'sha1',
    'log_filename': 'prov_test.log',
}
definitions_yaml = """
activity_description:
    function1:
        description: "set initial value of object1"
        parameters:
            - name: value
              description: "initial value"
              value: kwargs.value
        generation:
            - role: object1
              description: "output object"
              entity_description: Object
              value: object1
    function2:
        description: "set value of object2 using object1"
        parameters:
            - name: value
              description: "initial value"
              value: kwargs.value
        usage:
            - role: object1
              description: "input object"
              entity_description: Object
              value: object1
        generation:
            - role: object2
              description: "output object"
              entity_description: Object
              value: object2
entity_description:
    Object:
        description: "A Python variable in memory"
        type: PythonObject
    File:
        description: "A File on the disk"
        type: File
agent:
"""

definitions = yaml.safe_load(definitions_yaml)

prov_capture = logprov.capture.ProvCapture(definitions=definitions, config=provconfig)


class Object(object):

    value = 0

    def __repr__(self):
        return str(self.value)


@prov_capture.trace_methods
class Class1(object):

    def __init__(self):
        self.object1 = Object()
        self.object2 = Object()

    def __repr__(self):
        return "Class1"

    def function1(self, value=0):
        self.object1.value = value
        return self.object1

    def function2(self, value=0):
        self.object2.value = self.object1.value + value
        return self.object2

    def function3(self, value=0):
        self.object1.value = value
        return self.object1


start = datetime.datetime.now().isoformat()
c1 = Class1()
c1.function1(value=2)
#c1.function1(value=2)
#c1.function3(value=5)
c1.function2(value=2)
#c1.function3(value=2)
c1.function1(value=2)
c1.function2(value=2)
end = datetime.datetime.now().isoformat()

logname = provconfig['log_filename']
provlist = read_prov(logname=logname, start=start, end=end)
provdoc = provlist2provdoc(provlist)
# for pr in provdoc.get_records():
#     print(pr.get_provn())
provdoc.serialize(logname + '.json')
provdoc.serialize(logname + '.xml', format='xml')
provdoc2svg(provdoc, logname + '.svg')
