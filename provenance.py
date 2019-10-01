"""
Provenance-related functionality (from ctapipe initially)
"""

import json
import logging
import os
import platform
import sys
import uuid
from contextlib import contextmanager
from os.path import abspath
import psutil
from astropy.time import Time
from gammapy.utils.scripts import read_yaml
from pathlib import Path
from functools import wraps
import gammapy
import time
import datetime
import hashlib
import yaml
from prov.model import ProvDocument


log = logging.getLogger(__name__)

__all__ = ['LogProv']

_interesting_env_vars = [
    'CONDA_DEFAULT_ENV',
    'CONDA_PREFIX',
    'CONDA_PYTHON_EXE',
    'CONDA_EXE',
    'CONDA_PROMPT_MODIFIER',
    'CONDA_SHLVL',
    'PATH',
    'LD_LIBRARY_PATH',
    'DYLD_LIBRARY_PATH',
    'USER',
    'HOME',
    'SHELL',
]

CONFIG_PATH = Path(__file__).resolve().parent / "config"
SCHEMA_FILE = CONFIG_PATH / "definition_test.yaml"
definition = read_yaml(SCHEMA_FILE)

PROV_PREFIX = '_PROV_'

class LogProv(type):
    """ A Metaclass which decorates the methods with trace."""

    def __new__(cls, clsname, superclasses, attributedict):
        """ Every method gets decorated with the decorator trace."""

        for attr in attributedict:
            if attr in definition["activities"].keys() and callable(attributedict[attr]):
                print('decorated method:', attr)
                attributedict[attr] = trace(attributedict[attr])
        return type.__new__(cls, clsname, superclasses, attributedict)


def trace(func):
    """ A decorator which tracks provenance info."""

    @wraps(func)
    def wrapper(self, *args, **kwargs):
        # if analysis.settings["general"]["logging"]["level"] == "PROV":
        # p = Provenance()
        activity = func.__name__
        activity_id = id(func)
        start = datetime.datetime.now().isoformat()  # time.time()
        # Start event, log activity_id, start_time, parameter values, used entities
        if activity in definition["activities"].keys():
            # p.start_activity(activity)
            logprov(dict(activity_id=activity_id, activity_name=activity, startTime=start))
        # Run activity
        # TODO: add try and log if exception occured
        analysis = func(self, *args, **kwargs)
        end = datetime.datetime.now().isoformat()  # time.time()
        # Start event, log activity_id, start_time, parameter values, used entities
        if activity in definition["activities"].keys():
            # log activity parameters
            pdict = {}
            for p in definition["activities"][activity]["parameters"]:
                if 'name' in p and 'location' in p:
                    pv = get_nested_value(analysis, p["location"])
                    if pv:  # if pv is defined
                        pdict[p["name"]] = pv
            if pdict:
                logprov(dict(activity_id=activity_id, parameters=pdict))
            # log used entities
            for usage in definition["activities"][activity]["usage"]:
                # if 'from_parameter' in usage:
                #     for k, v in usage['from_parameter']:
                #         usage[k] = pdict[v]
                if 'location' in usage:
                    ue = get_nested_value(analysis, usage["location"])
                    if ue:  # if ue is defined
                        urole = usage.get("role", usage["location"])
                        eid = get_id(ue, usage)
                        provdict = dict(activity_id=activity_id, used_role=urole, used_id=eid)
                        if 'entityType' in usage:
                            provdict['entity_type'] = usage['entityType']
                            if provdict['entity_type'] not in ['PythonObject']:
                                if ue is not eid:
                                    provdict['entity_location'] = ue
                        logprov(provdict)
            #p.add_input_file("test.txt")
            # log generated entities
            for generation in definition["activities"][activity]["generation"]:
                if 'location' in generation:
                    ge = get_nested_value(analysis, generation["location"])
                    if ge:  # if ge is defined
                        grole = generation.get("role", generation["location"])
                        eid = get_id(ge, generation)
                        provdict = dict(activity_id=activity_id, generated_role=grole, generated_id=eid)
                        if 'entityType' in generation:
                            provdict['entity_type'] = generation['entityType']
                            if provdict['entity_type'] not in ['PythonObject']:
                                if ge is not eid:
                                    provdict['entity_location'] = ge
                        logprov(provdict)
                        if 'has_members' in generation:
                            list = get_nested_value(analysis, generation["has_members"].get('list'))
                            for elt in list:
                                eltval = get_nested_value(elt, generation["has_members"].get('location'))
                                eltid = get_id(eltval, generation["has_members"])
                                provdict = dict(entity_id=eid, member_id=eltid)
                                if 'entityType' in generation["has_members"]:
                                    provdict['member_type'] = generation["has_members"]['entityType']
                                    if provdict['member_type'] not in ['PythonObject']:
                                        if eltval is not eltid:
                                            provdict['member_location'] = eltval
                                logprov(provdict)
            # p.add_output_file("test.txt")
            logprov(dict(activity_id=activity_id, endTime=end))
            # p.finish_activity()
            # dump prov to file gammapy-prov in outdir

    return wrapper


def logprov(provdict):
    """ Write a dictionary to the log with a prefix to indicate provenance info"""
    log.info("{}{}".format(PROV_PREFIX, provdict))


def read_logprov(logname):
    """ Read a list of provenance dictionaries from the log"""
    provlist = []
    with open(logname, 'r') as f:
        for l in f.readlines():
            if PROV_PREFIX in l:
                provstr = l.split(PROV_PREFIX).pop()
                provdict = yaml.safe_load(provstr)
                provlist.append(provdict)
    return provlist


def provlist2provdoc(provlist):
    """ Convert a list of provenance dictionaries to a provdoc W3C PROV compatible"""
    pdoc = ProvDocument()
    pdoc.set_default_namespace('param:')
    pdoc.add_namespace('id', 'id:')
    records = {}
    for provdict in provlist:
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


def get_file_hash(path):
    # get hash of file
    fullpath = os.path.abspath(os.path.expandvars(path))
    if os.path.isfile(fullpath):
        BLOCKSIZE = 65536
        hasher = hashlib.md5()
        with open(fullpath, 'rb') as afile:
            buf = afile.read(BLOCKSIZE)
            while len(buf) > 0:
                hasher.update(buf)
                buf = afile.read(BLOCKSIZE)
        filehash = hasher.hexdigest()
        log.info("The entity is a file with hash={} ({})".format(filehash, path))
        return filehash
    else:
        log.warning("The entity is a file that was not found ({}).".format(path))
        return fullpath


def get_id(value, description):
    """Helper function that gets the id of an entity, depending on its type."""
    etype = description.get("entityType")
    etypes = definition["entityTypes"]
    if etype not in etypes:
        log.warning("The entity type {} was not found in the definitions.".format(etype))
    if etype == 'PythonObject':
        # value is an object in memory
        try:
            # identify with the hash of the object
            return abs(hash(value))
        except TypeError:
            # otherwise use id(), i.e. its memory address
            return abs(id(value))
    if 'File' in etype:
        # value is a path to a file, get the hash of this file
        return get_file_hash(value)
    if etype == 'DataStore':
        # value is a path to a Gammapy data store, get full path? get hash of index ?
        # return os.path.abspath(os.path.expandvars(value))
        return get_file_hash(os.path.join(value, 'obs-index.fits.gz'))
    # if no specific way to get id, use value as the id
    return value


def get_nested_value(nested, branch):
    """Helper function that gets a specific value in a nested dictionary or class."""
    list_branch = branch.split(".")
    leaf = list_branch.pop(0)
    if not nested:
        return globals().get(leaf, None)
    # Get value of leaf
    if isinstance(nested, dict):
        val = nested.get(leaf, None)
    elif isinstance(nested, object):
        val = getattr(nested, leaf, None)
    else:
        raise TypeError
    # Continue to explore leaf or return value
    if len(list_branch):
        str_branch = ".".join(list_branch)
        return get_nested_value(val, str_branch)
    else:
        if not val:
            val = globals().get(leaf, None)
        return val


def trace_nested_value(nested, branch, type, activity_id):
    """Helper function that logs a specific value in a nested dictionary or class."""
    list_branch = branch.split(".")
    leaf = list_branch.pop(0)
    str_branch = ".".join(list_branch)
    if isinstance(nested, dict):
        val = nested[leaf]
    elif isinstance(nested, object):
        val = nested.__getattribute__(leaf)
    else:
        raise TypeError
    if len(list_branch):
        trace_nested_value(val, str_branch, type, activity_id)
    else:
        if type == "param":
            log.info("{} param: {}={}".format(PROV_PREFIX, leaf, val))
        elif type == "used":
            log.info("{} used entity_id={}".format(PROV_PREFIX, id(val)))
            # log.info("used entity value: {}".format(val))
        elif type == "generated":
            log.info("{} generated entity_id={}".format(PROV_PREFIX, id(val)))
            #log.info("used entity value: {}".format(val))


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
        log.debug("added input entity '{}' to activity: '{}'".format(
            filename, self.current_activity.name))

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
        log.debug("added output entity '{}' to activity: '{}'".format(
            filename, self.current_activity.name))

    def add_config(self, config):
        """
        add configuration parameters to the current activity

        Parameters
        ----------
        config: dict
            configuration paramters
        """
        self.current_activity.register_config(config)

    def finish_activity(self, status='completed', activity_name=None):
        """ end the current activity """
        activity = self._activities.pop()
        if activity_name is not None and activity_name != activity.name:
            raise ValueError("Tried to end activity '{}', but '{}' is current "
                             "activity".format(activity_name, activity.name))

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
        """ returns provenence for full list of activities """
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
            'activity_name': activity_name,
            'activity_uuid': str(uuid.uuid4()),
            'start': {},
            'stop': {},
            'system': {},
            'input': [],
            'output': []
        }
        self.name = activity_name

    def start(self):
        """ begin recording provenance for this activity. Set's up the system
        and startup provenance data. Generally should be called at start of a
        program."""
        self._prov['start'].update(_sample_cpu_and_memory())
        self._prov['system'].update(_get_system_provenance())

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
        self._prov['input'].append(dict(url=url, role=role))

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
        self._prov['output'].append(dict(url=url, role=role))

    def register_config(self, config):
        """ add a dictionary of configuration parameters to this activity"""
        self._prov['config'] = config

    def finish(self, status='completed'):
        """ record final provenance information, normally called at shutdown."""
        self._prov['stop'].update(_sample_cpu_and_memory())

        # record the duration (wall-clock) for this activity
        t_start = Time(self._prov['start']['time_utc'], format='isot')
        t_stop = Time(self._prov['stop']['time_utc'], format='isot')
        self._prov['status'] = status
        self._prov['duration_min'] = (t_stop - t_start).to('min').value

    @property
    def output(self):
        return self._prov.get('output', None)

    @property
    def input(self):
        return self._prov.get('input', None)

    def sample_cpu_and_memory(self):
        """
        Record a snapshot of current CPU and memory information.
        """
        if 'samples' not in self._prov:
            self._prov['samples'] = []
        self._prov['samples'].append(_sample_cpu_and_memory())

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
            version=platform.version(),
            system=platform.system(),
            release=platform.release(),
            libcver=platform.libc_ver(),
            num_cpus=psutil.cpu_count(),
            boot_time=Time(psutil.boot_time(), format='unix').isot,
        ),
        python=dict(
            version_string=sys.version,
            version=platform.python_version_tuple(),
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