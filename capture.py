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
import yaml
from gammapy.scripts.info import (
    get_info_dependencies,
    get_info_version,
    get_info_envvar,
)

log = logging.getLogger(__name__)

__all__ = ["provenance"]

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
definition = yaml.safe_load(SCHEMA_FILE.read_text())

PROV_PREFIX = "_PROV_"

sessions = []
last_generated = {}


def provenance(cls):
    """A function decorator which decorates the methods with trace function."""
    for attr in cls.__dict__:
        if attr in definition["activities"].keys() and callable(getattr(cls, attr)):
            setattr(cls, attr, trace(getattr(cls, attr)))
    return cls


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
    """Check if provenance option is enabled in configuration settings."""

    active = True
    if activity not in definition["activities"].keys():
        active = False
    if not analysis:
        active = False
    # if not analysis.settings["general"]["logging"]["level"] == "PROV":
    #   active = False
    return active


def log_session(analysis, start):
    """Log start of a session."""

    session_id = abs(hash(analysis))
    session_name = f"{analysis.__class__.__module__}.{analysis.__class__.__name__}"
    if session_id not in sessions:
        # TODO serialise config
        sessions.append(session_id)
        system = get_system_provenance()
        log_record = {
            "session_id": session_id,
            "session_name": session_name,
            "startTime": start,
            "config": analysis.config.filename,
            "system": system,
        }
        log_prov_info(log_record)
    return session_id


def log_start_activity(activity, activity_id, session_id, start):
    """Log start of an activity."""

    log_record = {
        "activity_id": activity_id,
        "activity_name": activity,
        "in_session": session_id,
        "startTime": start,
    }
    log_prov_info(log_record)


def log_finish_activity(activity_id, end):
    """Log end of an activity."""

    log_record = {
        "activity_id": activity_id,
        "endTime": end
    }
    log_prov_info(log_record)


def log_parameters(analysis, activity, activity_id):
    """Log parameters and values."""

    parameter_list = definition["activities"][activity]["parameters"]
    if parameter_list:
        parameters = {}
        for parameter in parameter_list:
            if "name" in parameter and "value" in parameter:
                parameter_value = get_nested_value(analysis, parameter["value"])
                if parameter_value:
                    parameters[parameter["name"]] = parameter_value
        log_record = {
            "activity_id": activity_id,
            "parameters": parameters
        }
        log_prov_info(log_record) if parameters else False


def log_usage(analysis, activity, activity_id):
    """Log used entities."""

    usage_list = definition["activities"][activity]["usage"] or []
    for item in usage_list:
        if "value" in item and item["value"] in last_generated:
            props = {"id": last_generated[item["value"]]}
        else:
            props = get_item_properties(analysis, item)
        if "id" in props:
            log_record = {
                "activity_id": activity_id,
                "used_id": props["id"],
            }
            if "entityName" in item:
                log_record.update({"entity_type": item["entityName"]})
            if "location" in props:
                log_record.update({"entity_location": props["location"]})
            log_prov_info(log_record)


def log_generation(analysis, activity, activity_id):
    """Log generated entities."""

    generation_list = definition["activities"][activity]["generation"] or []
    for item in generation_list:
        props = get_item_properties(analysis, item)
        if "id" in props:
            if "value" in item:
                last_generated[item["value"]] = props["id"]
            log_record = {
                "activity_id": activity_id,
                "generated_id": props["id"],
            }
            if "entityName" in item:
                log_record.update({"entity_type": item["entityName"]})
            if "location" in props:
                log_record.update({"entity_location": props["location"]})
            log_prov_info(log_record)
            log_members(props["id"], item, analysis)
            log_derivations(props["id"], item, analysis)


def log_members(entity_id, item, analysis):
    """Log members of and entity."""

    if "has_members" in item:
        subitem = item["has_members"]
        generated_list = get_nested_value(analysis, subitem["list"]) or []
        for member in generated_list:
            props = get_item_properties(member, subitem)
            if "id" in props:
                log_record = {
                    "entity_id": entity_id,
                    "member_id": props["id"]
                }
                if "entityName" in subitem:
                    log_record.update({"member_type": subitem["entityName"]})
                if "location" in props:
                    log_record.update({"member_location": props["location"]})
                log_prov_info(log_record)


def log_derivations(entity_id, item, analysis):
    """Log members of and entity."""

    if "is_derived_from" in item:
        subitem = item["is_derived_from"]
        entity_list = get_nested_value(analysis, subitem["list"]) or []
        for entity in entity_list:
            props = get_item_properties(entity, subitem)
            if "id" in props:
                log_record = {
                    "entity_id": entity_id,
                    "derivation_id": props["id"]
                }
                if "location" in props:
                    log_record.update({"derivation_location": props["location"]})
                log_prov_info(log_record)


def log_prov_info(prov_dict):
    """Write a dictionary to the log."""

    log.info(
        "{}{}{}{}".format(
            PROV_PREFIX, datetime.datetime.now().isoformat(), PROV_PREFIX, prov_dict
        )
    )


def get_entity_id(value, item):
    """Helper function that gets the id of an entity, depending on its type."""

    try:
        entity_name = item["entityName"]
        entity_type = definition["entities"][entity_name]["type"]
    except Exception as ex:
        log.warning(f"{PROV_PREFIX}{str(ex)}")
        entity_name = ""
        entity_type = ""

    if entity_type == "FileCollection":
        filename = value
        index = definition["entities"][entity_name].get("index", "")
        if Path(os.path.expandvars(value)).is_dir() and index:
            filename = Path(value) / index
        return get_file_hash(filename)
    if entity_type == "File":
        return get_file_hash(value)

    try:
        return abs(hash(value) + hash(str(value)))
    except TypeError:
        # rk: two different objects may use the same memory address
        # so use hash(entity_name) to avoid issues
        return abs(id(value) + hash(entity_name))


def get_item_properties(nested, item):
    """Helper function that returns properties of an entity or member."""

    value = ""
    properties = {}
    if "id" in item:
        item_id = str(get_nested_value(nested, item["id"]))
        item_ns = item.get("namespace", None)
        if item_ns:
            item_id = item_ns + ":" + item_id
        properties["id"] = item_id
    if "role" in item:
        properties["role"] = item["role"]
    if "location" in item:
        properties["location"] = get_nested_value(nested, item["location"])
    if "value" in item:
        value = get_nested_value(nested, item["value"])
    if not value and "location" in properties:
        value = properties["location"]
    if value and "id" not in properties:
        properties["id"] = get_entity_id(value, item)
    return properties


def get_file_hash(path):
    """Helper function that returns hash of the content of a file."""

    full_path = Path(os.path.expandvars(path))
    if full_path.is_file():
        block_size = 65536
        hash_md5 = hashlib.md5()
        with open(full_path, "rb") as f:
            buffer = f.read(block_size)
            while len(buffer) > 0:
                hash_md5.update(buffer)
                buffer = f.read(block_size)
        file_hash = hash_md5.hexdigest()
        log.debug(f"{PROV_PREFIX}File entity {path} has hash {file_hash}")
        return file_hash
    else:
        log.warning(f"{PROV_PREFIX}File entity {path} not found")
        return full_path


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
        if "(" in leaf:
            leaf_elts = leaf.replace(")", "").split("(")
            leaf_func = leaf_elts.pop(0)
            leaf_args = {}
            for arg in leaf_elts:
                if "=" in arg:
                    k, v = arg.split("=")
                    leaf_args[k] = v.replace("\"", "")
            val = getattr(nested, leaf_func, lambda *args, **kwargs: None)(**leaf_args)
        else:
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


# ctapipe inherited code starts here
#


def get_system_provenance():
    """Return JSON string containing provenance for all things that are fixed during the runtime."""

    bits, linkage = platform.architecture()

    return dict(
        # gammapy specific
        version=get_info_version(),
        dependencies=get_info_dependencies(),
        envvars=get_info_envvar(),
        # gammapy specific
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
        environment=get_env_vars(),
        arguments=sys.argv,
        start_time_utc=Time.now().isot,
    )


def get_env_vars():
    """Return env vars defined at the main scope of the script."""

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
