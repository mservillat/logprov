"""
Provenance i/o conversion functions
"""

import datetime
import yaml
from prov.model import ProvDocument

PROV_PREFIX = "_PROV_"

__all__ = ["provlist2provdoc", "provdoc2svg", "read_prov"]

def provlist2provdoc(provlist):
    """ Convert a list of provenance dictionaries to a provdoc W3C PROV compatible"""
    pdoc = ProvDocument()
    pdoc.set_default_namespace('param:')
    pdoc.add_namespace('id', 'id:')
    records = {}
    for provdict in provlist:
        if 'session_id' in provdict:
            sess_id = 'id:' + str(provdict['session_id'])
            if sess_id in records:
                sess = records[sess_id]
            else:
                sess = pdoc.entity(sess_id)
                records[sess_id] = sess
            sess.add_attributes({
                'prov:label': provdict['session_name'],
                'prov:type': "SystemEnvironment",
                'prov:generatedAtTime': provdict['startTime'],
                #'system': str(provdict['system']),
            })
        # activity
        if 'activity_id' in provdict:
            act_id = 'id:' + str(provdict['activity_id']).replace('-','')
            if act_id in records:
                act = records[act_id]
            else:
                act = pdoc.activity(act_id)
                records[act_id] = act
            # activity name
            if 'activity_name' in provdict:
                act.add_attributes({'prov:label': provdict['activity_name']})
            # activity start
            if 'startTime' in provdict:
                act.set_time(startTime=datetime.datetime.fromisoformat(provdict['startTime']))
            # activity end
            if 'endTime' in provdict:
                act.set_time(endTime=datetime.datetime.fromisoformat(provdict['endTime']))
            # in session?
            if 'in_session' in provdict:
                sess_id = 'id:' + str(provdict['in_session'])
                pdoc.wasInfluencedBy(act_id, sess_id)  # , other_attributes={'prov:type': "Context"})
            # activity configuration
            if 'parameters' in provdict:
                params = {k: str(provdict['parameters'][k]) for k in provdict['parameters']}
                par = pdoc.entity(act_id + '_parameters', other_attributes=params)
                par.add_attributes({'prov:type': 'Parameters'})
                act.used(par, attributes={'prov:type': 'Setup'})
            # usage
            if 'used_id' in provdict:
                ent_id = 'id:' + str(provdict['used_id'])
                if ent_id in records:
                    ent = records[ent_id]
                else:
                    ent = pdoc.entity(ent_id)
                    records[ent_id] = ent
                if 'entity_type' in provdict:
                    ent.add_attributes({'prov:type': provdict['entity_type']})
                if 'entity_value' in provdict:
                    ent.add_attributes({'prov:value': str(provdict['entity_value'])})
                if 'entity_location' in provdict:
                    ent.add_attributes({'prov:location': str(provdict['entity_location'])})
                rol = provdict.get('used_role', None)
                # if rol:
                #     ent.add_attributes({'prov:label': rol})
                act.used(ent_id, attributes={'prov:role': rol})
            # generation
            if 'generated_id' in provdict:
                ent_id = 'id:' + str(provdict['generated_id'])
                if ent_id in records:
                    ent = records[ent_id]
                else:
                    ent = pdoc.entity(ent_id)
                    records[ent_id] = ent
                if 'entity_type' in provdict:
                    ent.add_attributes({'prov:type': provdict['entity_type']})
                if 'entity_value' in provdict:
                    ent.add_attributes({'prov:value': str(provdict['entity_value'])})
                if 'entity_location' in provdict:
                    ent.add_attributes({'prov:location': str(provdict['entity_location'])})
                rol = provdict.get('generated_role', None)
                # if rol:
                #     ent.add_attributes({'prov:label': rol})
                ent.wasGeneratedBy(act, attributes={'prov:role': rol})
        # entity
        if 'entity_id' in provdict:
            ent_id = 'id:' + str(provdict['entity_id'])
            if ent_id in records:
                ent = records[ent_id]
            else:
                ent = pdoc.entity(ent_id)
                records[ent_id] = ent
            if 'entity_name' in provdict:
                ent.add_attributes({'prov:label': provdict['entity_name']})
            if 'entity_type' in provdict:
                ent.add_attributes({'prov:type': provdict['entity_type']})
            if 'entity_value' in provdict:
                ent.add_attributes({'prov:value': str(provdict['entity_value'])})
            if 'entity_location' in provdict:
                ent.add_attributes({'prov:location': str(provdict['entity_location'])})
            # member
            if 'member_id' in provdict:
                mem_id = 'id:' + str(provdict['member_id'])
                if mem_id in records:
                    mem = records[mem_id]
                else:
                    mem = pdoc.entity(mem_id)
                    records[mem_id] = mem
                if 'member_type' in provdict:
                    mem.add_attributes({'prov:type': provdict['member_type']})
                if 'member_value' in provdict:
                    mem.add_attributes({'prov:value': str(provdict['member_value'])})
                if 'member_location' in provdict:
                    ent.add_attributes({'prov:location': str(provdict['member_location'])})
                ent.hadMember(mem)
        # agent
    return pdoc


def provdoc2svg(provdoc, filename):
    from prov.dot import prov_to_dot
    from pydotplus.graphviz import InvocationException
    try:
        dot = prov_to_dot(provdoc, use_labels=True, show_element_attributes=True, show_relation_attributes=True)
        svg_content = dot.create(format="svg")
    except InvocationException as e:
        svg_content = ""
        print('problem while creating svg content')
    with open(filename, "wb") as f:
        f.write(svg_content)


def read_prov(logname, start=None, end=None):
    """ Read a list of provenance dictionaries from the log"""
    if start:
        start_dt = datetime.datetime.fromisoformat(start)
    if end:
        end_dt = datetime.datetime.fromisoformat(start)
    prov_list = []
    with open(logname, 'r') as f:
        for l in f.readlines():
            if PROV_PREFIX in l:
                ll = l.split(PROV_PREFIX)
                prov_str = ll.pop()
                try:
                    prov_dt = datetime.datetime.fromisoformat(ll.pop())
                except ValueError as e:
                    prov_dt = None
                keep = True
                if prov_dt:
                    if start and prov_dt < start_dt:
                        keep = False
                    if end and prov_dt > end_dt:
                        keep = False
                if keep:
                    prov_dict = yaml.safe_load(prov_str)
                    prov_list.append(prov_dict)
    return prov_list
