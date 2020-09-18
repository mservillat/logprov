"""
Provenance capture functions (from ctapipe and gammapy initially)
"""
import datetime
import hashlib
import logging
import logging.config
import os
import platform
import sys
import uuid
from functools import wraps
from pathlib import Path
import psutil
import yaml
import inspect

__all__ = ["ProvCapture"]

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

PROV_PREFIX = "_PROV_"
SUPPORTED_HASH_TYPE = ["sha1", "sha224", "sha256", "sha384", "sha512", "md5"]  # included in hashlib

logging_default_config = {
    'version': 1,
    'formatters': {
        'simple': {
            'format': '%(levelname)s %(message)s',
            'datefmt': '%Y-%m-%dT%H:%M:%S',
        },
    },
    'handlers': {
        'provHandler': {
            'class': 'logging.handlers.WatchedFileHandler',
            'level': 'INFO',
            'formatter': 'simple',
            'filename': 'prov.log',
        },
    },
    'loggers': {
        'provLogger': {
            'level': 'INFO',
            'handlers': ['provHandler'],
            'propagate': False,
        },
    },
    'disable_existing_loggers': False,
}

# To be provided at class init, here are the default values:
logprov_default_config = {
    'capture': True,
    'hash_type': 'sha1',
    'log_filename': 'prov.log',
    'system_dict': {},
    'env_vars': {},
}

definitions_default = {
    "activity_description": {},
    "entity_description": {},
    "agent": {},
}


# Read config and definitions from files (yaml)


def read_config(filename):
    """Read yaml config file"""
    filename_path = Path(filename)
    prov_config = yaml.safe_load(filename_path.read_text())
    return prov_config


def read_definitions(filename):
    """Read yaml definition file"""
    filename_path = Path(filename)
    prov_definitions = yaml.safe_load(filename_path.read_text())
    return prov_definitions


# Capture class

class ProvCapture(object):

    def __init__(self, definitions=None, config=None):
        if config:
            self.config = config
        else:
            self.config = logprov_default_config
        # logging dict may be given in the config, otherwise, used default
        if "logging" in self.config:
            self.logging_dict = self.config["logging"]
        else:
            if "log_filename" in self.config:
                logging_default_config['handlers']['provHandler']['filename'] = self.config["log_filename"]
            self.logging_dict = logging_default_config
        # Check config and set to default if undefined
        for key in logprov_default_config:
            if key not in self.config:
                self.config[key] = logprov_default_config[key]
        # Set logger
        self.logger = self.get_logger()
        if definitions:
            self.definitions = definitions
        else:
            self.definitions = definitions_default
        # global variables
        self.sessions = []
        self.traced_entities = {}

    # Logger configuration

    def get_logger(self):
        """Initialize logger."""
        try:
            logging.config.dictConfig(self.logging_dict)
        except Exception as ex:
            print(str(ex))
            print('Failed to set up the logger.')
            logging.basicConfig(level="INFO")
        return logging.getLogger('provLogger')

    def set_log_filename(self, log_filename):
        """Set log filename in config and in logging dict."""
        self.config['log_filename'] = log_filename
        self.logger.handlers[0].baseFilename = log_filename

    def log_is_active(self, class_instance, activity):
        """Check if provenance option is enabled in configuration settings."""
        active = True
        # if activity not in self.definitions["activity_description"].keys():
        #     active = False
        if not class_instance:
            active = False
        if "capture" not in self.config or not self.config["capture"]:
            active = False
        return active

    # Decorators

    def trace_methods(self, cls):
        """A function decorator which decorates the methods with trace function."""
        for attr in cls.__dict__:
            if not attr.startswith('_') and callable(getattr(cls, attr)):
                setattr(cls, attr, self.trace(getattr(cls, attr)))
        return cls

    def trace(self, func):
        """A decorator which tracks provenance info."""

        if func.__name__ not in self.definitions["activity_description"]:
            self.logger.warning(f'No definition for function {func.__name__}')
            # TODO: try to create a definition automatically (may not link used/wgb entities though...)

        @wraps(func)
        def wrapper(*args, **kwargs):

            activity = func.__name__
            activity_id = self.get_activity_id()
            class_instance = args[0]
            class_instance.args = args
            class_instance.kwargs = kwargs

            # provenance capture before execution
            derivation_records = self.get_derivation_records(class_instance, activity)
            parameter_records = self.get_parameters_records(class_instance, activity, activity_id)
            usage_records = self.get_usage_records(class_instance, activity, activity_id)

            # activity execution
            start = datetime.datetime.now().isoformat()
            result = func(*args, **kwargs)
            end = datetime.datetime.now().isoformat()

            # no provenance logging
            if not self.log_is_active(class_instance, activity):
                return result
            # provenance logging only if activity ends properly
            session_id = self.log_session(class_instance, start)
            for prov_record in derivation_records:
                self.log_prov_record(prov_record)
            self.log_start_activity(activity, activity_id, session_id, start)
            for prov_record in parameter_records:
                self.log_prov_record(prov_record)
            for prov_record in usage_records:
                self.log_prov_record(prov_record)
            self.log_generation(class_instance, activity, activity_id)
            self.log_finish_activity(activity_id, end)
            return result

        return wrapper

    # ID management

    @staticmethod
    def get_activity_id():
        # uuid example: ea5caa9f-0a76-42f5-a1a7-43752df755f0
        # uuid[-12:]: 43752df755f0
        # uuid[-6:]: f755f0
        return str(uuid.uuid4())[-6:]

    def get_hash_method(self):
        """Helper function that returns hash method used."""
        try:
            method = self.config["hash_type"].lower()
        except KeyError as ex:
            method = "sha1"
        if method not in SUPPORTED_HASH_TYPE:
            self.logger.warning(f"Hash method {method} not supported")
            method = "Full path"
        return method

    def get_file_hash(self, path):
        """Helper function that returns hash of the content of a file."""
        method = self.get_hash_method()
        full_path = Path(os.path.expandvars(path))
        if method == "Full path":
            return str(full_path)
        if full_path.is_file():
            block_size = 65536
            hash_func = getattr(hashlib, method)()
            with open(full_path, "rb") as f:
                buffer = f.read(block_size)
                while len(buffer) > 0:
                    hash_func.update(buffer)
                    buffer = f.read(block_size)
            file_hash = hash_func.hexdigest()
            self.logger.debug(f"File entity {path} has {method} hash {file_hash}")
            return file_hash
        else:
            self.logger.warning(f"File entity {path} not found")
            return path

    def get_entity_id(self, value, item):
        """Helper function that makes the id of an entity, depending on its type."""
        try:
            entity_name = item["entityName"]
            entity_type = self.definitions["entity_description"][entity_name]["type"]
        except KeyError as ex:
            # self.logger.warning(f"{repr(ex)} in {item}")
            entity_name = ""
            entity_type = ""

        if entity_type == "FileCollection":
            filename = value
            index = self.definitions["entity_description"][entity_name].get("index", "")
            if Path(os.path.expandvars(value)).is_dir() and index:
                filename = Path(value) / index
            return self.get_file_hash(filename)
        if entity_type == "File":
            return self.get_file_hash(value)
        # entity is not a File (must be a PythonObject)
        try:
            entity_id = abs(hash(value) + hash(str(value)))
            if hasattr(value, "entity_version"):
                entity_id += getattr(value, "entity_version")
            return entity_id
        except TypeError:
            # two different objects may use the same memory address
            # so use hash(entity_name) to avoid issues
            return abs(id(value) + hash(entity_name))

    def get_nested_value(self, nested, branch):
        """Helper function that gets a specific value in a nested dictionary or class."""
        list_branch = branch.split(".")
        leaf = list_branch.pop(0)
        # return value of leaf
        if not nested:
            return globals().get(leaf, None)
        # get value of leaf
        if isinstance(nested, dict):
            val = nested.get(leaf, None)
        elif isinstance(nested, object):
            if "(" in leaf:  # leaf is a function
                leaf_elements = leaf.replace(")", "").replace(" ", "").split("(")
                leaf_arg_list = leaf_elements.pop().split(",")
                leaf_func = leaf_elements.pop()
                leaf_args = []
                leaf_kwargs = {}
                for arg in leaf_arg_list:
                    if "=" in arg:
                        k, v = arg.split("=")
                        leaf_kwargs[k] = v.replace('"', "")
                    elif arg:
                        leaf_args.append(arg.replace('"', ""))
                val = getattr(nested, leaf_func, lambda *args, **kwargs: None)(*leaf_args, **leaf_kwargs)
            else:                                               # leaf is an attribute
                val = getattr(nested, leaf, None)
        else:
            raise TypeError
        # continue to explore branch
        if len(list_branch):
            str_branch = ".".join(list_branch)
            return self.get_nested_value(val, str_branch)
        # return value of leaf
        if not val:
            val = globals().get(leaf, None)
        return val

    def get_item_properties(self, nested, item):
        """Helper function that returns properties of an entity or member."""
        try:
            entity_name = item["entityName"]
            entity_type = self.definitions["entity_description"][entity_name]["type"]
        except Exception as ex:
            self.logger.warning(f"{repr(ex)} in {item}")
            entity_name = ""
            entity_type = ""
        value = ""
        properties = {}
        if "id" in item:
            item_id = str(self.get_nested_value(nested, item["id"]))
            item_ns = item.get("namespace", None)
            if item_ns:
                item_id = item_ns + ":" + item_id
            properties["id"] = item_id
        if "location" in item:
            properties["location"] = self.get_nested_value(nested, item["location"])
        if "value" in item:
            value = self.get_nested_value(nested, item["value"])
        if not value and "location" in properties:
            value = properties["location"]
        if "overwrite" in item:
            # Add or increment entity_version to make value a different entity
            if hasattr(value, "entity_version"):
                version = getattr(value, "entity_version")
                version += 1
                setattr(value, "entity_version", version)
            else:
                try:
                    setattr(value, "entity_version", 1)
                except AttributeError as ex:
                    self.logger.warning(f"{repr(ex)} for {value}")
        if value and "id" not in properties:
            properties["id"] = self.get_entity_id(value, item)
            if "File" in entity_type and properties["id"] != value:
                method = self.get_hash_method()
                properties["hash"] = properties["id"]
                properties["hash_type"] = method
        if entity_name:
            properties["name"] = entity_name
            for attr in ["type", "contentType"]:
                if attr in self.definitions["entity_description"][entity_name]:
                    properties[attr] = self.definitions["entity_description"][entity_name][attr]
        if "location" in properties and properties["location"]:
            properties["location"] = os.path.expandvars(properties["location"])
        return properties

    # Log records

    def log_prov_record(self, prov_dict):
        """Write a dictionary to the logger."""
        record_date = datetime.datetime.now().isoformat()
        self.logger.info(f"{PROV_PREFIX}{record_date}{PROV_PREFIX}{prov_dict}")

    def log_session(self, class_instance, start):
        """Log start of a session."""
        session_id = abs(hash(class_instance))
        module_name = class_instance.__class__.__module__
        class_name = class_instance.__class__.__name__
        session_name = f"{module_name}.{class_name}"
        if session_id not in self.sessions:
            self.sessions.append(session_id)
            system = self.get_system_provenance()
            # TODO: add agent with os.getlogin() + relation to session
            prov_record = {
                "session_id": session_id,
                "name": session_name,
                "startTime": start,
                "configFile": class_instance.config.filename,
                "system": system,
            }
            self.log_prov_record(prov_record)
        return session_id

    def log_start_activity(self, activity, activity_id, session_id, start):
        """Log start of an activity."""
        prov_record = {
            "activity_id": activity_id,
            "name": activity,
            "startTime": start,
            "in_session": session_id,
            "agent_name": os.getlogin(),
        }
        self.log_prov_record(prov_record)

    def log_finish_activity(self, activity_id, end):
        """Log end of an activity."""
        prov_record = {
            "activity_id": activity_id,
            "endTime": end
        }
        self.log_prov_record(prov_record)

    def get_derivation_records(self, class_instance, activity):
        """Get log records for potentially derived entity."""
        records = []
        for var, pair in self.traced_entities.items():
            entity_id, item = pair
            value = self.get_nested_value(class_instance, var)
            new_id = self.get_entity_id(value, item)
            if new_id != entity_id:
                prov_record = {
                    "entity_id": new_id,
                    "progenitor_id": entity_id
                }
                records.append(prov_record)
                self.traced_entities[var] = (new_id, item)
                self.logger.warning(f"Derivation detected by {activity} for {var}. ID: {new_id}")
        return records

    def get_parameters_records(self, class_instance, activity, activity_id):
        """Get log records for parameters of the activity."""
        records = []
        parameter_list = []
        if activity in self.definitions["activity_description"]:
            parameter_list = self.definitions["activity_description"][activity]["parameters"] or []
        if parameter_list:
            parameters = {}
            for parameter in parameter_list:
                if "name" in parameter and "value" in parameter:
                    parameter_value = self.get_nested_value(class_instance, parameter["value"])
                    if parameter_value:
                        parameters[parameter["name"]] = parameter_value
            if parameters:
                prov_record = {
                    "activity_id": activity_id,
                    "parameters": parameters
                }
                records.append(prov_record)
        return records

    def get_usage_records(self, class_instance, activity, activity_id):
        """Get log records for each usage of the activity."""
        records = []
        usage_list = []
        if activity in self.definitions["activity_description"]:
            if "usage" in self.definitions["activity_description"][activity]:
                usage_list = self.definitions["activity_description"][activity]["usage"] or []
        for item in usage_list:
            props = self.get_item_properties(class_instance, item)
            if "id" in props:
                entity_id = props.pop("id")
                if "namespace" in props:
                    entity_id = props.pop("namespace") + ":" + entity_id
                # Usage record
                prov_record = {
                    "activity_id": activity_id,
                    "used_id": entity_id,
                }
                if "role" in item:
                    prov_record.update({"used_role": item["role"]})
                # Entity record
                prov_record_ent = {
                    "entity_id": entity_id,
                }
                if "entityName" in item:
                    prov_record_ent.update({"name": item["entityName"]})
                for prop in props:
                    prov_record_ent.update({prop: props[prop]})
                records.append(prov_record_ent)
                records.append(prov_record)
        return records

    def log_generation(self, class_instance, activity, activity_id):
        """Log generated entities."""
        generation_list = []
        if activity in self.definitions["activity_description"]:
            generation_list = self.definitions["activity_description"][activity]["generation"] or []
        for item in generation_list:
            props = self.get_item_properties(class_instance, item)
            if "id" in props:
                entity_id = props.pop("id")
                if "namespace" in props:
                    entity_id = props.pop("namespace") + ":" + entity_id
                # Generation record
                if "value" in item:
                    self.traced_entities[item["value"]] = (entity_id, item)
                prov_record = {
                    "activity_id": activity_id,
                    "generated_id": entity_id,
                }
                if "role" in item:
                    prov_record.update({"generated_role": item["role"]})
                # Entity record
                prov_record_ent = {
                    "entity_id": entity_id,
                }
                if "entityName" in item:
                    prov_record_ent.update({"name": item["entityName"]})
                for prop in props:
                    prov_record_ent.update({prop: props[prop]})
                self.log_prov_record(prov_record_ent)
                self.log_prov_record(prov_record)
                if "has_members" in item:
                    self.log_members(entity_id, item["has_members"], class_instance)
                if "has_progenitors" in item:
                    self.log_progenitors(entity_id, item["has_progenitors"], class_instance)

    def log_members(self, entity_id, subitem, class_instance):
        """Log members of and entity."""
        if "list" in subitem:
            member_list = self.get_nested_value(class_instance, subitem["list"]) or []
        else:
            member_list = [class_instance]
        for member in member_list:
            props = self.get_item_properties(member, subitem)
            if "id" in props:
                mem_id = props.pop("id")
                # Record membership
                prov_record = {
                    "entity_id": entity_id,
                    "member_id": mem_id,
                }
                # Record entity
                prov_record_ent = {
                    "entity_id": mem_id,
                }
                if "entityName" in subitem:
                    prov_record_ent.update({"name": subitem["entityName"]})
                for prop in props:
                    prov_record_ent.update({prop: props[prop]})
                self.log_prov_record(prov_record_ent)
                self.log_prov_record(prov_record)

    def log_progenitors(self, entity_id, subitem, class_instance):
        """Log progenitors of and entity."""
        if "list" in subitem:
            progenitor_list = self.get_nested_value(class_instance, subitem["list"]) or []
        else:
            progenitor_list = [class_instance]
        for entity in progenitor_list:
            props = self.get_item_properties(entity, subitem)
            if "id" in props:
                progen_id = props.pop("id")
                # Record progenitor link
                prov_record = {
                    "entity_id": entity_id,
                    "progenitor_id": progen_id,
                }
                # Record entity
                prov_record_ent = {
                    "entity_id": progen_id,
                }
                for prop in props:
                    prov_record_ent.update({prop: props[prop]})
                self.log_prov_record(prov_record_ent)
                self.log_prov_record(prov_record)

    def log_file_generation(self, file_path, entity_name="", used=None, role="", activity_name=""):
        # get file properties
        if os.path.isfile(file_path):
            item = dict(
                file_path=file_path,
                entityName=entity_name,
            )
            entity_id = self.get_entity_id(file_path, item)
            prov_record = {
                "entity_id": entity_id,
                "name": entity_name,
                "location": file_path,
                "hash": entity_id,
                "hash_type": self.config["hash_type"],
            }
            self.log_prov_record(prov_record)
            if activity_name:
                activity_id = self.get_activity_id()
                prov_record = {
                    "activity_id": activity_id,
                    "name": activity_name,
                }
                self.log_prov_record(prov_record)
                if used:
                    for used_entity in used:
                        used_id = self.get_entity_id(used_entity, {})
                        prov_record = {
                            "activity_id": activity_id,
                            "used_id": used_id,
                        }
                        self.log_prov_record(prov_record)
                    prov_record = {
                        "activity_id": activity_id,
                        "generated_id": entity_id,
                    }
                if role:
                    prov_record.update({"generated_role": role})
                self.log_prov_record(prov_record)
            else:
                if used:
                    for used_entity in used:
                        used_id = self.get_entity_id(used_entity, {})
                        prov_record = {
                            "entity_id": entity_id,
                            "progenitor_id": used_id,
                        }
                        self.log_prov_record(prov_record)

    def get_system_provenance(self):
        """Return JSON string containing provenance for all things that are fixed during the runtime."""
        bits, linkage = platform.architecture()
        system_dict = dict(
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
                boot_time=datetime.datetime.fromtimestamp(psutil.boot_time()).isoformat(),
            ),
            python=dict(
                version_string=sys.version,
                version=platform.python_version(),
                compiler=platform.python_compiler(),
                implementation=platform.python_implementation(),
            ),
            environment=self.get_env_vars(),
            arguments=sys.argv,
            start_time_utc=datetime.datetime.now().isoformat(),
        )
        # Include additional dict provided
        if 'system_dict' in self.config and self.config['system_dict']:
            system_dict.update(self.config['system_dict'])
        return system_dict

    def get_env_vars(self):
        """Return env vars defined at the main scope of the script."""
        env_vars = {}
        for var in _interesting_env_vars:
            env_vars[var] = os.getenv(var, None)
        # Include additional vars requested
        if 'env_vars' in self.config and self.config['env_vars']:
            for var in self.config['env_vars']:
                env_vars[var] = os.getenv(var, None)
        return env_vars

    def _sample_cpu_and_memory(self):
        # times = np.asarray(psutil.cpu_times(percpu=True))
        # mem = psutil.virtual_memory()

        return dict(
            time_utc=datetime.datetime.utcnow().isoformat(),
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
