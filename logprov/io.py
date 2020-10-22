"""
Provenance i/o conversion functions
"""

import datetime
import yaml
from prov.model import ProvDocument

PROV_PREFIX = "_PROV_"
DEFAULT_NS = "session"

__all__ = ["provlist2provdoc", "provdoc2svg", "read_prov"]

# TODO: prov with internal or external ids (no ns or ns+session)


def provlist2provdoc(provlist, default_ns=DEFAULT_NS):
    """ Convert a list of provenance dictionaries to a provdoc W3C PROV compatible"""
    pdoc = ProvDocument()
    pdoc.set_default_namespace("param:")
    pdoc.add_namespace(default_ns, default_ns + ":")
    pdoc.add_namespace("voprov", "voprov:")
    records = {}
    sess_id = ""
    for provdict in provlist:
        if "session_id" in provdict:
            sess_id = str(provdict.pop("session_id"))
            sess_qid = default_ns + ":" + sess_id
            if sess_id in records:
                sess = records[sess_qid]
            else:
                sess = pdoc.entity(sess_qid)
                records[sess_qid] = sess
            sess.add_attributes(
                {
                    "prov:label": "LogProvSession",
                    "prov:type": "LogProvSession",
                    "prov:generatedAtTime": provdict.pop("startTime"),
                    #'configFile': provdict.pop('configFile'),
                    'module': str(provdict.pop('module')),
                    'class': str(provdict.pop('class')),
                    'system': str(provdict.pop('system'))[:50],
                    'definitions': str(provdict.pop('definitions'))[:50],
                }
            )
        # activity
        if "activity_id" in provdict:
            act_id = default_ns + ":" + "_".join([sess_id, str(provdict.pop("activity_id")).replace("-", "")])
            if act_id in records:
                act = records[act_id]
            else:
                act = pdoc.activity(act_id)
                records[act_id] = act
            # activity name
            if "name" in provdict:
                act.add_attributes({"prov:label": provdict.pop("name")})
            # activity start
            if "startTime" in provdict:
                act.set_time(
                    startTime=datetime.datetime.fromisoformat(provdict.pop("startTime"))
                )
            # activity end
            if "endTime" in provdict:
                act.set_time(
                    endTime=datetime.datetime.fromisoformat(provdict.pop("endTime"))
                )
            # in session?
            # if "in_session" in provdict:
            #     sess_qid = default_ns + ":" + str(provdict.pop("in_session"])
            #     pdoc.wasInfluencedBy(
            #         act_id, sess_id
            #     )  # , other_attributes={'prov:type': "Context"})
            # activity configuration
            if "agent_name" in provdict:
                agent_id = str(provdict.pop("agent_name"))
                if ":" not in agent_id:
                    agent_id = default_ns + ":" + agent_id
                else:
                    new_ns = agent_id.split(":").pop(0)
                    pdoc.add_namespace(new_ns, new_ns + ":")
                if agent_id in records:
                    agent = records[agent_id]
                else:
                    agent = pdoc.agent(agent_id)
                    records[agent_id] = agent
                act.wasAssociatedWith(agent, attributes={"prov:role": "Operator"})
            if "parameters" in provdict:
                params_record = provdict.pop("parameters")
                params = {
                    k: str(params_record[k]) for k in params_record
                }
                # par_id = act_id + "_parameters"
                # par = pdoc.entity(par_id, other_attributes=params)
                # par.add_attributes({"prov:type": "Parameters"})
                # par.add_attributes({"prov:label": "WasConfiguredBy"})
                # act.used(par, attributes={"prov:type": "Setup"})
                for name, value in params.items():
                    value_short = str(value)[:20]
                    if len(value_short) == 20:
                        value_short += "..."
                    par = pdoc.entity(act_id + "_" + name)
                    par.add_attributes({"prov:label": name + " = " + value_short})
                    par.add_attributes({"prov:type": "voprov:Parameter"})
                    par.add_attributes({"voprov:name": name})
                    par.add_attributes({"prov:value": value_short})
                    act.used(par, attributes={"prov:type": "Setup"})
            # usage
            if "used_id" in provdict:
                ent_id = str(provdict.pop("used_id"))
                if ":" not in ent_id:
                    ent_id = default_ns + ":" + "_".join([sess_id, ent_id])
                else:
                    new_ns = ent_id.split(":").pop(0)
                    pdoc.add_namespace(new_ns, new_ns + ":")
                if ent_id in records:
                    ent = records[ent_id]
                else:
                    ent = pdoc.entity(ent_id)
                    records[ent_id] = ent
                rol = provdict.pop("used_role", None)
                # if rol:
                #     ent.add_attributes({'prov:label': rol})
                act.used(ent, attributes={"prov:role": rol})
            # generation
            if "generated_id" in provdict:
                ent_id = str(provdict.pop("generated_id"))
                if ":" not in ent_id:
                    ent_id = default_ns + ":" + "_".join([sess_id, ent_id])
                else:
                    new_ns = ent_id.split(":").pop(0)
                    pdoc.add_namespace(new_ns, new_ns + ":")
                if ent_id in records:
                    ent = records[ent_id]
                else:
                    ent = pdoc.entity(ent_id)
                    records[ent_id] = ent
                rol = provdict.pop("generated_role", None)
                # if rol:
                #     ent.add_attributes({'prov:label': rol})
                ent.wasGeneratedBy(act, attributes={"prov:role": rol})
            for k, v in provdict.items():
                act.add_attributes({k: str(v)})
        # entity
        if "entity_id" in provdict:
            ent_id = str(provdict.pop("entity_id"))
            label = ""
            if ":" not in ent_id:
                ent_id = default_ns + ":" + "_".join([sess_id, ent_id])
            else:
                new_ns = ent_id.split(":").pop(0)
                pdoc.add_namespace(new_ns, new_ns + ":")
            if ent_id in records:
                ent = records[ent_id]
            else:
                ent = pdoc.entity(ent_id)
                records[ent_id] = ent
            if "name" in provdict:
                label = provdict.pop("name")
                ent.add_attributes({"voprov:name": label})
            if "entity_description" in provdict:
                label = provdict.pop("entity_description")
                ent.add_attributes({"voprov:entity_description": label})
            if "type" in provdict:
                ent.add_attributes({"prov:type": provdict.pop("type")})
            if "value" in provdict:
                value_short = str(provdict.pop("value"))[:20]
                if len(value_short) == 20:
                    value_short += "..."
                ent.add_attributes({"prov:value": value_short})
            if "location" in provdict:
                location = str(provdict.pop("location"))
                ent.add_attributes({"prov:location": location})
                if label:
                    label = label + " in " + location
            if label:
                ent.add_attributes({"prov:label": label})
            if "generated_time" in provdict:
                ent.add_attributes({"prov:generatedAtTime": str(provdict.pop("generated_time"))})
            # member
            if "member_id" in provdict:
                mem_id = str(provdict.pop("member_id"))
                if ":" not in mem_id:
                    mem_id = default_ns + ":" + "_".join([sess_id, mem_id])
                else:
                    new_ns = mem_id.split(":").pop(0)
                    pdoc.add_namespace(new_ns, new_ns + ":")
                if mem_id in records:
                    mem = records[mem_id]
                else:
                    mem = pdoc.entity(mem_id)
                    records[mem_id] = mem
                ent.hadMember(mem)
            if "progenitor_id" in provdict:
                progen_id = str(provdict.pop("progenitor_id"))
                if ":" not in progen_id:
                    progen_id = default_ns + ":" + "_".join([sess_id, progen_id])
                else:
                    new_ns = progen_id.split(":").pop(0)
                    pdoc.add_namespace(new_ns, new_ns + ":")
                if progen_id in records:
                    progen = records[progen_id]
                else:
                    progen = pdoc.entity(progen_id)
                    records[progen_id] = progen
                ent.wasDerivedFrom(progen)
            for k, v in provdict.items():
                ent.add_attributes({k: str(v)})
        # agent
    return pdoc


def provdoc2svg(provdoc, filename):
    from prov.dot import prov_to_dot
    from pydotplus.graphviz import InvocationException

    try:
        dot = prov_to_dot(
            provdoc,
            use_labels=True,
            show_element_attributes=True,
            show_relation_attributes=True,
        )
        svg_content = dot.create(format="svg")
    except InvocationException as e:
        svg_content = ""
        print(f"problem while creating svg content: {repr(e)}")
    with open(filename, "wb") as f:
        f.write(svg_content)


def read_prov(logname="prov.log", start=None, end=None, prefix=PROV_PREFIX):
    """ Read a list of provenance dictionaries from the structured log"""
    if start:
        start_dt = datetime.datetime.fromisoformat(start)
    if end:
        end_dt = datetime.datetime.fromisoformat(end)
    prov_list = []
    with open(logname, "r") as f:
        for l in f.readlines():
            ll = l.split(prefix)
            if len(ll) >= 2:
                prov_str = ll.pop()
                prov_dt = datetime.datetime.fromisoformat(ll.pop())
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
