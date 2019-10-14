"""
Provenance-related functionality (from ctapipe initially)
"""

import datetime
import hashlib
import json
import logging
import os
import platform
import sys
import uuid
from contextlib import contextmanager
from functools import wraps
from os.path import abspath
from pathlib import Path
from astropy.time import Time
import psutil
import yaml
from prov.model import ProvDocument
import gammapy
from gammapy.utils.scripts import read_yaml

log = logging.getLogger(__name__)

__all__ = ["LogProv"]

_interesting_env_vars = [
    "CONDA_DEFAULT_ENV",
    "CONDA_PREFIX",
    "CONDA_PYTHON_EXE",
    "CONDA_EXE",
    "CONDA_PROMPT_MODIFIER",
    "CONDA_SHLVL",
    "PATH",
    "LD_LIBRARY_PATH",
    "DYLD_LIBRARY_PATH",
    "USER",
    "HOME",
    "SHELL",
]

CONFIG_PATH = Path(__file__).resolve().parent / "config"
SCHEMA_FILE = CONFIG_PATH / "definition.yaml"
definition = read_yaml(SCHEMA_FILE)

PROV_PREFIX = "_PROV_"
sessions = []


class LogProv(type):
    """A metaclass which decorates the methods with trace function."""

    def __new__(mcs, clsname, superclasses, attributedict):
        """Every method gets decorated with the decorator trace."""

        for attr in attributedict:
            if attr in definition["activities"].keys() and callable(attributedict[attr]):
                print("decorated method:", attr)
                attributedict[attr] = trace(attributedict[attr])
        return type.__new__(mcs, clsname, superclasses, attributedict)


def trace(func):
    """A decorator which tracks provenance info."""

    @wraps(func)
    def wrapper(self, *args, **kwargs):
        activity = func.__name__
        start = datetime.datetime.now().isoformat()
        # log start activity
        # p.start_activity(activity)
        activity_id = abs(id(func) + id(start))     # Python memory id
        log_start_activity(activity, activity_id, start)
        try:
            analysis = func(self, *args, **kwargs)
        except Exception as e:
            # log end and error
            end = datetime.datetime.now().isoformat()
            log_finish_activity(activity_id, end, status='ERROR', msg=str(e))
            raise
        if not log_is_active(analysis, activity):
            return True
        end = datetime.datetime.now().isoformat()
        analysis.args = args
        analysis.kwargs = kwargs
        # log session and environment info
        session_id = abs(hash(analysis))
        if not session_id in sessions:
            sessions.append(session_id)
            # log session start, environment and configfile
            session_name = ""
            try:
                session_name = str(analysis.__class__).split("'")[1]
            except Exception as e:
                log.warning("Cannot get session name")
                pass
            config = getattr(getattr(analysis, "config", None), "filename", "")
            system = _get_system_provenance()
            log_record = {
                "session_id": session_id,
                "session_name": session_name,
                "startTime": start,
                "config": str(config),
                "system": system,
            }
            log_prov(log_record)
        log_prov({"activity_id": activity_id, "in_session": session_id})
        # log parameters
        # p.add_parameters(parameters)
        log_parameters(analysis, activity, activity_id)
        # log used entities
        # p.add_input_file("test.txt")
        log_usage(analysis, activity, activity_id)
        # log generated entities and members
        # p.add_output_file("test.txt")
        log_generation(analysis, activity, activity_id)
        # log finish activity
        # p.finish_activity(activity)
        log_finish_activity(activity_id, end)
        #
        # dump prov to file gammapy-prov in outdir

    return wrapper


def log_is_active(analysis, activity):
    active = True
    # active = Provenance()
    if activity not in definition["activities"].keys():
        active = False
    if not analysis:
        active = False
    # if not analysis.settings["general"]["logging"]["level"] == "PROV":
    #   active = False
    return active


def log_start_activity(activity, activity_id, start):
    log_record = {
        "activity_id": activity_id,
        "activity_name": activity,
        "startTime": start,
    }
    log_prov(log_record)


def log_finish_activity(activity_id, end, **kwargs):
    log_record = {
        "activity_id": activity_id,
        "endTime": end
    }
    for k in kwargs:
        log_record[k] = kwargs[k]
    log_prov(log_record)


def log_parameters(analysis, activity, activity_id):
    parameter_list = definition["activities"][activity]["parameters"]
    if parameter_list:
        parameters = {}
        for parameter in parameter_list:
            if "name" in parameter and "value" in parameter:
                parameter_value = get_nested_value(analysis, parameter["value"])
                # parameter_value is found
                if parameter_value:
                    parameters[parameter["name"]] = parameter_value
        log_record = {"activity_id": activity_id, "parameters": parameters}
        # use filter if defined
        if parameters:
            log_prov(log_record)


def log_usage(analysis, activity, activity_id):
    usage_list = definition["activities"][activity]["usage"]
    if usage_list:
        for item in usage_list:
            item_id = ""
            item_role = ""
            item_value = ""
            item_location = ""
            if "id" in item:
                item_id = get_nested_value(analysis, item["id"])
            if "value" in item:
                item_value = get_nested_value(analysis, item["value"])
            if "location" in item:
                item_location = get_nested_value(analysis, item["location"])
                if not item_value:
                    item_value = item_location
            if item_value:
                item_role = item.get("role", item_value)
                if not item_id:
                    item_id = get_entity_id(item_value, item)
            if item_id:
                log_record = {
                    "activity_id": activity_id,
                    "used_role": item_role,
                    "used_id": item_id,
                }
                if "entityType" in item:
                    log_record.update({"entity_type": item['entityType']})
                if item_location:
                    log_record.update({"entity_location": item_location})
                # use filter if defined
                log_prov(log_record)


def log_generation(analysis, activity, activity_id):
    generation_list = definition["activities"][activity]["generation"]
    if generation_list:
        for item in generation_list:
            item_id = ""
            item_role = ""
            item_value = ""
            item_location = ""
            if "id" in item:
                item_id = get_nested_value(analysis, item["id"])
            if "value" in item:
                item_value = get_nested_value(analysis, item["value"])
            if "location" in item:
                item_location = get_nested_value(analysis, item["location"])
                if not item_value:
                    item_value = item_location
            if item_value:
                item_role = item.get("role", item_value)
                if not item_id:
                    item_id = get_entity_id(item_value, item)
            if item_id:
                log_record = {
                    "activity_id": activity_id,
                    "generated_role": item_role,
                    "generated_id": item_id,
                }
                if "entityType" in item:
                    log_record.update({"entity_type": item['entityType']})
                if item_location:
                    log_record.update({"entity_location": item_location})
                # use filter if defined
                log_prov(log_record)
            # log members in generated entities
            # p.add_members()
            if "has_members" in item:
                subitem = item["has_members"]
                generated_list = get_nested_value(analysis, subitem["list"])
                element_id = ""
                element_value = ""
                element_location = ""
                if not generated_list:
                    return False
                for element in generated_list:
                    if "id" in subitem:
                        element_id = get_nested_value(element, subitem["id"])
                    if "value" in subitem:
                        element_value = get_nested_value(element, subitem["value"])
                    if "location" in subitem:
                        element_location = get_nested_value(element, subitem["location"])
                        if not element_value:
                            element_value = element_location
                    if element_value:
                        if not element_id:
                            element_id = get_entity_id(element_value, item)
                    if element_id:
                        log_record = {
                            "entity_id": item_id,
                            "member_id": element_id,
                        }
                        if "entityType" in item["has_members"]:
                            log_record.update({"member_type": item["has_members"]['entityType']})
                        if item_location:
                            log_record.update({"member_location": element_location})
                        log_prov(log_record)


def log_prov(prov_dict):
    """ Write a dictionary to the log with a prefix to indicate provenance info"""
    log.info("{}{}{}{}".format(PROV_PREFIX, datetime.datetime.now().isoformat(), PROV_PREFIX, prov_dict))


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


def get_file_hash(path):
    # get hash of file
    full_path = os.path.abspath(os.path.expandvars(path))
    if os.path.isfile(full_path):
        block_size = 65536
        hash_md5 = hashlib.md5()
        with open(full_path, "rb") as f:
            buffer = f.read(block_size)
            while len(buffer) > 0:
                hash_md5.update(buffer)
                buffer = f.read(block_size)
        file_hash = hash_md5.hexdigest()
        log.info(f"{PROV_PREFIX}The entity is a file with hash={file_hash} ({path})")
        return file_hash
    else:
        log.warning(f"{PROV_PREFIX}The entity is a file that was not found ({path})")
        return full_path


def get_entity_id(value, description):
    """Helper function that gets the id of an entity, depending on its type."""
    entity_name = description.get("entityType", None)
    entity_type = ""
    entity_names = definition["entities"]
    if entity_name and entity_name in entity_names:
        entity_type = entity_names[entity_name].get("type", None)
    else:
        log.warning(f"{PROV_PREFIX}Entity {entity_name} not found in definitions")
    if "FileCollection" in entity_type:
        # value for e.g. DataStore is a path to a Gammapy data store, get full path? get hash of index?
        index = entity_names[entity_name].get("index", "")
        return get_file_hash(os.path.join(value, index))
    if "File" in entity_type:
        # value is a path to a file .- get the hash of this file
        return get_file_hash(value)
    # if no specific way to get id, try hash() of python object, or id()
    try:
        # identify with the hash of the object
        return abs(hash(value))
    except TypeError:
        # otherwise use id() i.e. its memory address
        # rk: two different objects may use the same memory address, so use hash(entity_type) to avoid issues
        return abs(id(value) + hash(entity_name))


def get_nested_value(nested, branch):
    """Helper function that gets a specific value in a nested dictionary or class."""
    list_branch = branch.split(".")
    leaf = list_branch.pop(0)
    if not nested:
        return globals().get(leaf, None)
    # get value of leaf
    if isinstance(nested, dict):
        val = nested.get(leaf, None)
    elif isinstance(nested, object):
        val = getattr(nested, leaf, None)
    else:
        raise TypeError
    # continue to explore leaf or return value
    if len(list_branch):
        str_branch = ".".join(list_branch)
        return get_nested_value(val, str_branch)
    else:
        if not val:
            val = globals().get(leaf, None)
        return val


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



# def trace_nested_value(nested, branch, type, activity_id):
#     """Helper function that logs a specific value in a nested dictionary or class."""
#     list_branch = branch.split(".")
#     leaf = list_branch.pop(0)
#     str_branch = ".".join(list_branch)
#     if isinstance(nested, dict):
#         val = nested[leaf]
#     elif isinstance(nested, object):
#         val = nested.__getattribute__(leaf)
#     else:
#         raise TypeError
#     if len(list_branch):
#         trace_nested_value(val, str_branch, type, activity_id)
#     else:
#         if type == "param":
#             log.info(f"{PROV_PREFIX} param: {leaf}={val}")
#         elif type == "used":
#             log.info(f"{PROV_PREFIX} used entity_id={id(val)}")
#         elif type == "generated":
#             log.info("f{PROV_PREFIX} generated entity_id={id(val)}")

# ctapipe inherited code starts here ---------
#
#
#
class Singleton(type):
    """" metaclass for singleton pattern """

    instance = None

    def __call__(cls, *args, **kw):
        if not cls.instance:
            cls.instance = super().__call__(*args, **kw)
        return cls.instance


class Provenance(metaclass=Singleton):
    """
    Manage the provenance info for a stack of *activities*

    use `start_activity(name)` to start an activity. Any calls to
    `add_input_entity()` or `add_output_entity()` will register files within
    that activity. Finish the current activity with `finish_activity()`.

    Nested activities are allowed, and handled as a stack. The final output
    is not hierarchical, but a flat list of activities (however hierarchical
    activities could easily be implemented if necessary)
    """

    def __init__(self):
        self._activities = []  # stack of active activities
        self._finished_activities = []

    def start_activity(self, activity_name=sys.executable):
        """ push activity onto the stack"""
        activity = _ActivityProvenance(activity_name)
        activity.start()
        self._activities.append(activity)
        log.info(f"started activity: {activity_name}")

    def add_input_file(self, filename, role=None):
        """ register an input to the current activity

        Parameters
        ----------
        filename: str
            name or url of file
        role: str
            role this input file satisfies (optional)
        """
        self.current_activity.register_input(abspath(filename), role=role)
        log.debug(
            "added input entity '{}' to activity: '{}'".format(
                filename, self.current_activity.name
            )
        )

    def add_output_file(self, filename, role=None):
        """
        register an output to the current activity

        Parameters
        ----------
        filename: str
            name or url of file
        role: str
            role this output file satisfies (optional)

        """
        self.current_activity.register_output(abspath(filename), role=role)
        log.debug(
            "added output entity '{}' to activity: '{}'".format(
                filename, self.current_activity.name
            )
        )

    def add_config(self, config):
        """
        add configuration parameters to the current activity

        Parameters
        ----------
        config: dict
            configuration parameters
        """
        self.current_activity.register_config(config)

    def finish_activity(self, status="completed", activity_name=None):
        """ end the current activity """
        activity = self._activities.pop()
        if activity_name is not None and activity_name != activity.name:
            raise ValueError(
                "Tried to end activity '{}', but '{}' is current "
                "activity".format(activity_name, activity.name)
            )

        activity.finish(status)
        self._finished_activities.append(activity)
        log.debug(f"finished activity: {activity.name}")

    @contextmanager
    def activity(self, name):
        """ context manager for activities """
        self.start_activity(name)
        yield
        self.finish_activity(name)

    @property
    def current_activity(self):
        if len(self._activities) == 0:
            log.debug("No activity has been started... starting a default one")
            self.start_activity()
        return self._activities[-1]  # current activity as at the top of stack

    @property
    def finished_activities(self):
        return self._finished_activities

    @property
    def provenance(self):
        """ returns provenance for full list of activities """
        return [x.provenance for x in self._finished_activities]

    def as_json(self, **kwargs):
        """ return all finished provenance as JSON.  Kwargs for `json.dumps`
        may be included, e.g. `indent=4`"""

        def set_default(obj):
            """ handle sets (not part of JSON) by converting to list"""
            if isinstance(obj, set):
                return list(obj)

        return json.dumps(self.provenance, default=set_default, **kwargs)

    @property
    def active_activity_names(self):
        return [x.name for x in self._activities]

    @property
    def finished_activity_names(self):
        return [x.name for x in self._finished_activities]

    def clear(self):
        """ remove all tracked activities """
        self._activities = []
        self._finished_activities = []


class _ActivityProvenance:
    """
    Low-level helper class to collect provenance information for a given
    *activity*.  Users should use `Provenance` as a top-level API,
    not this class directly.
    """

    def __init__(self, activity_name=sys.executable):
        self._prov = {
            "activity_name": activity_name,
            "activity_uuid": str(uuid.uuid4()),
            "start": {},
            "stop": {},
            "system": {},
            "input": [],
            "output": [],
        }
        self.name = activity_name

    def start(self):
        """ begin recording provenance for this activity. Set's up the system
        and startup provenance data. Generally should be called at start of a
        program."""
        self._prov["start"].update(_sample_cpu_and_memory())
        self._prov["system"].update(_get_system_provenance())

    def register_input(self, url, role=None):
        """
        Add a URL of a file to the list of inputs (can be a filename or full
        url, if no URL specifier is given, assume 'file://')

        Parameters
        ----------
        url: str
            filename or url of input file
        role: str
            role name that this input satisfies
        """
        self._prov["input"].append(dict(url=url, role=role))

    def register_output(self, url, role=None):
        """
        Add a URL of a file to the list of outputs (can be a filename or full
        url, if no URL specifier is given, assume 'file://')

        Parameters
        ----------
        url: str
            filename or url of output file
        role: str
            role name that this output satisfies
        """
        self._prov["output"].append(dict(url=url, role=role))

    def register_config(self, config):
        """ add a dictionary of configuration parameters to this activity"""
        self._prov["config"] = config

    def finish(self, status="completed"):
        """ record final provenance information, normally called at shutdown."""
        self._prov["stop"].update(_sample_cpu_and_memory())

        # record the duration (wall-clock) for this activity
        t_start = Time(self._prov["start"]["time_utc"], format="isot")
        t_stop = Time(self._prov["stop"]["time_utc"], format="isot")
        self._prov["status"] = status
        self._prov["duration_min"] = (t_stop - t_start).to("min").value

    @property
    def output(self):
        return self._prov.get("output", None)

    @property
    def input(self):
        return self._prov.get("input", None)

    def sample_cpu_and_memory(self):
        """
        Record a snapshot of current CPU and memory information.
        """
        if "samples" not in self._prov:
            self._prov["samples"] = []
        self._prov["samples"].append(_sample_cpu_and_memory())

    @property
    def provenance(self):
        return self._prov


def _get_system_provenance():
    """ return JSON string containing provenance for all things that are
    fixed during the runtime"""

    bits, linkage = platform.architecture()

    return dict(
        gammapy_version=gammapy.__version__,
        gammapy_data_path=os.getenv("GAMMAPY_DATA"),
        executable=sys.executable,
        platform=dict(
            architecture_bits=bits,
            architecture_linkage=linkage,
            machine=platform.machine(),
            processor=platform.processor(),
            node=platform.node(),
            version=str(platform.version()),
            system=platform.system(),
            release=platform.release(),
            libcver=str(platform.libc_ver()),
            num_cpus=psutil.cpu_count(),
            boot_time=Time(psutil.boot_time(), format="unix").isot,
        ),
        python=dict(
            version_string=sys.version,
            version=platform.python_version(),
            compiler=platform.python_compiler(),
            implementation=platform.python_implementation(),
        ),
        environment=_get_env_vars(),
        arguments=sys.argv,
        start_time_utc=Time.now().isot,
    )


def _get_env_vars():
    envvars = {}
    for var in _interesting_env_vars:
        envvars[var] = os.getenv(var, None)
    return envvars


def _sample_cpu_and_memory():
    # times = np.asarray(psutil.cpu_times(percpu=True))
    # mem = psutil.virtual_memory()

    return dict(
        time_utc=Time.now().utc.isot,
        # memory=dict(total=mem.total,
        #             inactive=mem.inactive,
        #             available=mem.available,
        #             free=mem.free,
        #             wired=mem.wired),
        # cpu=dict(ncpu=psutil.cpu_count(),
        #          user=list(times[:, 0]),
        #          nice=list(times[:, 1]),
        #          system=list(times[:, 2]),
        #          idle=list(times[:, 3])),
    )
