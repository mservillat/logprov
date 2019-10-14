"""
Provenance capture functions (from ctapipe initially)
"""

import datetime
import hashlib
import logging
import os
import platform
import sys
from functools import wraps
from pathlib import Path
from astropy.time import Time
import psutil
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
    def wrapper(*args, **kwargs):

        # activity execution
        activity = func.__name__
        start = datetime.datetime.now().isoformat()
        activity_id = abs(id(func) + id(start))
        analysis = func(*args, **kwargs)
        end = datetime.datetime.now().isoformat()

        # no provenance logging
        if not log_is_active(analysis, activity):
            return True

        # provenance logging only if activity ends properly
        analysis.args = args
        analysis.kwargs = kwargs
        session_id = log_session(analysis, start)
        log_start_activity(activity, activity_id, session_id, start)
        log_parameters(analysis, activity, activity_id)
        log_usage(analysis, activity, activity_id)
        log_generation(analysis, activity, activity_id)
        log_finish_activity(activity_id, end)

    return wrapper


def log_is_active(analysis, activity):
    active = True
    if activity not in definition["activities"].keys():
        active = False
    if not analysis:
        active = False
    # if not analysis.settings["general"]["logging"]["level"] == "PROV":
    #   active = False
    return active


def log_session(analysis, start):
    session_id = abs(hash(analysis))
    session_name = f"{analysis.__class__.__module__}.{analysis.__class__.__name__}"
    if session_id not in sessions:
        sessions.append(session_id)
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
    return session_id


def log_start_activity(activity, activity_id, session_id, start):
    log_record = {
        "activity_id": activity_id,
        "activity_name": activity,
        "in_session": session_id,
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
                    log_record.update({"entity_type": item["entityType"]})
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
                    log_record.update({"entity_type": item["entityType"]})
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
                            log_record.update(
                                {"member_type": item["has_members"]["entityType"]}
                            )
                        if item_location:
                            log_record.update({"member_location": element_location})
                        log_prov(log_record)


def log_prov(prov_dict):
    """ Write a dictionary to the log with a prefix to indicate provenance info"""
    log.info("{}{}{}{}".format(PROV_PREFIX, datetime.datetime.now().isoformat(), PROV_PREFIX, prov_dict))


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


# ctapipe inherited code starts here ---------
#
#
#


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
