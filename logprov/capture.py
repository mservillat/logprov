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
import inspect
import uuid
from functools import wraps
from pathlib import Path
import psutil
import yaml

__all__ = ["read_config", "read_definitions", "ProvCapture"]

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
            'level': 'DEBUG',
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
    'log_args': True,
    'log_args_as_entities': True,
    'log_kwargs': True,
    'log_returned_result': True,
    'system_dict': {},
    'env_vars': {},
}

definitions_default = {
    "activity_descriptions": {},
    "entity_descriptions": {},
    "agents": {},
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

class Singleton(type):
    """" metaclass for singleton pattern """

    instance = None

    def __call__(cls, *args, **kw):
        if not cls.instance:
            cls.instance = super().__call__(*args, **kw)
        return cls.instance


class ProvCapture(metaclass=Singleton):

    def __init__(self, definitions=None, config=None, get_file_id_func=None):
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
        self.get_file_id_func = get_file_id_func
        # Set logger
        self.logger = self.get_logger()
        if definitions:
            self.definitions = definitions
        else:
            self.definitions = definitions_default
        # global variables
        self.sessions = []
        self.traced_variables = {}
        self.traced_returned_results = {}
        self.usage_ids = []
        self.globals = {}

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

    def log_is_active(self, scope, activity):
        """Check if provenance option is enabled in configuration settings."""
        active = True
        # if activity not in self.definitions["activity_descriptions"].keys():
        #     active = False
        if not scope:
            active = False
        if "capture" not in self.config or not self.config["capture"]:
            active = False
        return active

    # Decorators

    def trace_methods(self, cls):
        """A function decorator which decorates all methods with the trace() function."""
        for attr in cls.__dict__:
            if not attr.startswith('_') and callable(getattr(cls, attr)):
                setattr(cls, attr, self.trace(getattr(cls, attr)))
        return cls

    def trace(self, func):
        """A decorator which tracks provenance info."""

        if func.__name__ not in self.definitions["activity_descriptions"]:
            self.logger.warning(f'No definition for function {func.__name__}')
            # TODO: try to create a definition automatically (may not link used/wgb entities though...)

        @wraps(func)
        def wrapper(*args, **kwargs):

            activity = func.__name__
            activity_id = self.gen_activity_id()
            self.globals = {k: func.__globals__[k] for k in func.__globals__.keys() if k[0:1] is not '_'}
            # and k not in ['In', 'Out', 'exit', 'quit', 'provconfig', 'definitions_yaml', 'definitions']}
            # TODO: use inspect.ismethod()
            if ("method" in str(type(func))) or (len(args) > 0 and hasattr(args[0], "__dict__")):
                # func is a class method, search entities in class instance self (arg[0] of the method)
                self.logger.debug(f"{activity} is a class method")
                scope = args[0]
                scope.args = args
                scope.kwargs = kwargs
            else:
                # func is a regular function, search entities in globals
                scope = self.globals
                scope["args"] = args
                scope["kwargs"] = kwargs

            log_active = self.log_is_active(scope, activity)

            # provenance capture before execution
            if log_active:
                derivation_records = self.get_derivation_records(scope, activity)
                sig = inspect.signature(func)
                usage_records = self.get_usage_records(scope, activity, activity_id)
                parameter_records = self.get_parameters_records(scope, activity, activity_id, func_signature=sig)

            # activity execution
            start = datetime.datetime.now().isoformat()
            result = func(*args, **kwargs)
            end = datetime.datetime.now().isoformat()

            # provenance capture after execution
            if log_active:
                # rk: provenance logging only if activity ends properly
                session_id = self.log_session(scope, start)
                for prov_record in derivation_records:
                    self.log_prov_record(prov_record)
                self.log_start_activity(activity, activity_id, session_id, start)
                for prov_record in parameter_records:
                    self.log_prov_record(prov_record)
                for prov_record in usage_records:
                    self.log_prov_record(prov_record)
                self.log_generation(scope, activity, activity_id, result=result)
                self.log_finish_activity(activity_id, end)

            return result

        return wrapper

    # ID management

    @staticmethod
    def gen_activity_id():
        # uuid example: ea5caa9f-0a76-42f5-a1a7-43752df755f0
        # uuid[-12:]: 43752df755f0
        # uuid[-6:]: f755f0
        return str(uuid.uuid4())[-6:]

    def get_hash_method(self):
        """Helper function that returns hash method used."""
        try:
            method = self.config["hash_type"].lower()
        except KeyError as ex:
            method = logprov_default_config["hash_type"]
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

    def get_entity_id(self, value, item_description, var_name=""):
        """Helper function that guesses the id of an entity, depending on its type."""
        # Get entity description name and type
        try:
            ed_name = item_description["entity_description"]
            ed_type = self.definitions["entity_descriptions"][ed_name]["type"]
        except KeyError as ex:
            # self.logger.warning(f"{repr(ex)} in {item_description}")
            ed_name = var_name
            ed_type = ""
        # TODO: add list of ed_name + function to get id
        # If FileCollection: id = index file hash (value is the dir name)
        if ed_type == "FileCollection":
            filename = value
            index = self.definitions["entity_descriptions"][ed_name].get("index", "")
            if Path(os.path.expandvars(value)).is_dir() and index:
                filename = Path(value) / index
            if self.get_file_id_func:
                # use external function if defined
                return self.get_file_id_func(value)
            return self.get_file_hash(filename)
        # If File: id = file hash  (value is the file name)
        if ed_type == "File":
            if self.get_file_id_func:
                # use external function if defined
                return self.get_file_id_func(value)
            return self.get_file_hash(value)
        # entity is not a File (so must be a PythonObject)
        try:
            # id is defined as the hash of value (hash of the variable) plus the hash of its representation
            entity_id = (abs(hash(value) + hash(str(value)))) * 10
            # Add modifier for traced variables
            if "value" in item_description:
                if item_description["value"] in self.traced_variables:
                    entity_id += self.traced_variables[item_description["value"]]["modifier"]
            # Add entity_version if present (NOT USED - TO REMOVE)
            if hasattr(value, "entity_version"):
                entity_id += getattr(value, "entity_version")
            return entity_id
        except TypeError:
            # value may not have a hash()... then use id()
            # however, two different objects may use the same memory address
            # so add hash(ed_name) to avoid issues
            entity_id = (abs(id(value) + hash(ed_name))) * 10
            # Add modifier for traced variables
            if "value" in item_description:
                if item_description["value"] in self.traced_variables:
                    entity_id += self.traced_variables[item_description["value"]]["modifier"]
            return entity_id

    def get_nested_value(self, scope, branch):
        """Helper function that gets a specific value in a nested dictionary or class."""
        branch_list = branch.split(".")
        leaf = branch_list.pop(0)
        value = None
        if not scope:
            # Try to find leaf in globals (no scope to explore)
            value = self.globals.get(leaf, None)
            if value is not None:
                self.logger.debug(f"Found {leaf} in globals (no object or dict to search)")
            else:
                self.logger.warning(f"Not found: {leaf} (no object or dict to search)")
            return value
        # Get value of leaf in dict
        if isinstance(scope, dict):
            value = scope.get(leaf, None)
            if value is not None:
                self.logger.debug(f"Found {leaf} in a dict")
        # Get value of leaf in object
        elif isinstance(scope, object):
            if "(" in leaf:
                # leaf is a function
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
                value = getattr(scope, leaf_func, lambda *args, **kwargs: None)(*leaf_args, **leaf_kwargs)
            elif "[" in leaf:
                # leaf is list or dict
                leaf_elements = leaf.replace("]", "").replace(" ", "").split("[")
                leaf_index = int(leaf_elements.pop())
                leaf_list = getattr(scope, leaf_elements.pop())
                value = getattr(leaf_list, "__getitem__", lambda *args, **kwargs: None)(leaf_index)
            else:
                # leaf is an attribute
                value = getattr(scope, leaf, None)
            if value is not None:
                self.logger.debug(f"Found {leaf} in an object")
        else:
            raise TypeError
        # Continue to explore branch
        if len(branch_list):
            branch_str = ".".join(branch_list)
            return self.get_nested_value(value, branch_str)
        # No more branch to explore
        if value is None:
            # Try to find leaf in globals (not found in scope)
            value = self.globals.get(leaf, None)
            if value is not None:
                self.logger.debug(f"Found {leaf} in globals")
            else:
                self.logger.warning(f"Not found: {leaf}")
        return value

    def get_item_properties(self, scope, item_description):
        """Helper function that returns properties of an entity or member."""
        # Get entity description name and type
        try:
            ed_name = item_description["entity_description"]
            ed_type = self.definitions["entity_descriptions"][ed_name]["type"]
        except Exception as ex:
            self.logger.warning(f"{repr(ex)} in {item_description}")
            ed_name = ""
            ed_type = ""
        value = None
        properties = {}
        # item has an id to be resolved
        if "id" in item_description:
            properties["id"] = str(self.get_nested_value(scope, item_description["id"]))
        # item has a location to be resolved
        if "location" in item_description:
            properties["location"] = self.get_nested_value(scope, item_description["location"])
        # item has a value to be resolved
        if "value" in item_description:
            value = self.get_nested_value(scope, item_description["value"])
        # Copy location to value
        if value is None and "location" in properties:
            value = properties["location"]
        # Get id from value if no id was found
        if value is not None and "id" not in properties:
            properties["id"] = self.get_entity_id(value, item_description)
            # If File/FileCollection: keep hash and hash_type as properties
            if "File" in ed_type and properties["id"] != value:
                method = self.get_hash_method()
                properties["hash"] = properties["id"]
                properties["hash_type"] = method
        # Add namespace if not already done
        if "namespace" in item_description:
            item_id = properties["id"]
            item_ns = item_description["namespace"]
            if ":" not in item_id:
                properties["id"] = f"{item_ns}:{item_id}"
        # If PythonObject: keep value as properties (-->ValueEntity)
        if value is not None and ed_type == "PythonObject":
            properties["value"] = str(value)
        # Keep description attributes as properties
        if ed_name:
            properties["entity_description"] = ed_name
            for attr in ["type", "contentType"]:
                if attr in self.definitions["entity_descriptions"][ed_name]:
                    properties[attr] = self.definitions["entity_descriptions"][ed_name][attr]
        # Expand location to get absolute path
        if "location" in properties and properties["location"]:
            properties["location"] = os.path.expandvars(properties["location"])
        return properties

    # Log records

    def log_prov_record(self, prov_dict):
        """Write a dictionary to the logger."""
        record_date = datetime.datetime.now().isoformat()
        self.logger.info(f"{PROV_PREFIX}{record_date}{PROV_PREFIX}{prov_dict}")

    def log_session(self, scope, start):
        """Log start of a session."""
        # if isinstance(scope, dict):
        #     session_id = abs(hash(globals()['__name__']))
        # elif isinstance(scope, object):
        #     session_id = abs(hash(scope))
        # else:
        #     raise TypeError
        session_id = abs(hash(self))
        if session_id not in self.sessions:
            module_name = scope.__class__.__module__
            class_name = scope.__class__.__name__
            session_name = f"{module_name}.{class_name}"
            self.sessions.append(session_id)
            system = self.get_system_provenance()
            # TODO: add agent with os.getlogin() + relation to session
            prov_record = {
                "session_id": session_id,
                "module": module_name,
                "class": class_name,
                "startTime": start,
                #"configFile": scope.config.filename,
                "system": system,
                "definitions": self.definitions,
            }
            self.log_prov_record(prov_record)
        return session_id

    def log_start_activity(self, activity, activity_id, session_id, start):
        """Log start of an activity."""
        # TODO: add relation to agent from session
        prov_record = {
            "activity_id": activity_id,
            "name": activity,
            "startTime": start,
            "in_session": session_id,
            "agent_name": os.getlogin(),
        }
        self.log_prov_record(prov_record)
        return prov_record

    def log_finish_activity(self, activity_id, end):
        """Log end of an activity."""
        prov_record = {
            "activity_id": activity_id,
            "endTime": end
        }
        self.log_prov_record(prov_record)
        return prov_record

    def get_derivation_records(self, scope, activity):
        """Get log records for potentially derived entity."""
        records = []
        for var, tv_dict in self.traced_variables.items():
            if var != "_returned_result":
                entity_id = tv_dict["last_id"]
                value = self.get_nested_value(scope, var)
                new_id = self.get_entity_id(value, tv_dict["item_description"])
                if new_id != entity_id:
                    modifier = tv_dict["modifier"]
                    previous_ids = tv_dict["previous_ids"]
                    while new_id in previous_ids:
                        modifier += 1
                        new_id += 1
                        self.logger.warning(f'id has already been taken by this variable'
                                            f' ({var} {entity_id}): '
                                            f'update modifier to {modifier}')
                    previous_ids.append(new_id)
                    self.traced_variables[var] = {
                        "last_id": new_id,
                        "previous_ids": previous_ids,
                        "item_description": tv_dict["item_description"],
                        "modifier": modifier,
                    }
                    # Entity record
                    prov_record_ent = {
                        "entity_id": new_id,
                    }
                    if "entity_description" in tv_dict["item_description"]:
                        prov_record_ent.update({"entity_description": tv_dict["item_description"]["entity_description"]})
                    if "type" in tv_dict["item_description"]:
                        prov_record_ent.update({"type": tv_dict["item_description"]["type"]})
                    if "value" in tv_dict["item_description"]:
                        prov_record_ent.update({"location": tv_dict["item_description"]["value"]})
                    if modifier:
                        prov_record_ent.update({"modifier": modifier})
                    records.append(prov_record_ent)
                    # Derivation record
                    prov_record = {
                        "entity_id": new_id,
                        "progenitor_id": entity_id,
                        "generated_time": datetime.datetime.now().isoformat(),
                    }
                    records.append(prov_record)
                    self.logger.warning(f"Derivation detected by {activity} for {var}. ID: {new_id}")
        return records

    def get_parameters_records(self, scope, activity, activity_id, func_signature=None):
        """Get log records for parameters of the activity."""
        records = []
        parameter_list = []
        parameters = {}
        if activity in self.definitions["activity_descriptions"]:
            if "parameters" in self.definitions["activity_descriptions"][activity]:
                parameter_list = self.definitions["activity_descriptions"][activity]["parameters"] or []
        if parameter_list:
            for parameter in parameter_list:
                if "value" in parameter:
                    pvalue = self.get_nested_value(scope, parameter["value"])
                    if pvalue is not None:
                        pname = parameter.get("name", parameter["value"])
                        parameters[pname] = pvalue
        sig_args = []
        sig_kwargs = {}
        if func_signature and (self.config['log_args'] or self.config['log_kwargs']):
            for pname, p in func_signature.parameters.items():
                if pname == "self" or p.default == p.empty:
                    sig_args.append(pname)
                else:
                    sig_kwargs[pname] = p.default
        if self.config['log_args']:
            args = []
            # Get args in scope
            if hasattr(scope, "args"):
                args = scope.args
            elif "args" in scope:
                args = scope["args"]
            for i, pvalue in enumerate(args):
                pname = f"args[{str(i)}]"
                # Get name of arg from signature
                if len(sig_args) > i:
                    pname = "args." + str(sig_args[i])
                if pname != "args.self":
                    if self.config['log_args_as_entities']:
                        # Get id
                        var_name = pname.split("args.")[-1]
                        entity_id = self.get_entity_id(pvalue, {}, var_name=var_name)
                        if entity_id not in self.usage_ids:
                            # Usage record
                            prov_record = {
                                "activity_id": activity_id,
                                "used_id": entity_id,
                                "used_role": var_name,
                            }
                            records.append(prov_record)
                    else:
                        parameters[pname] = pvalue
        if self.config['log_kwargs']:
            kwargs = {}
            if hasattr(scope, "kwargs"):
                kwargs = scope.kwargs
            elif "kwargs" in scope:
                kwargs = scope["kwargs"]
            for pname, pvalue in sig_kwargs.items():
                if pname in kwargs:
                    pvalue = kwargs[pname]
                parameters["kwargs." + pname] = pvalue
        if parameters:
            prov_record = {
                "activity_id": activity_id,
                "parameters": parameters
            }
            records.append(prov_record)
        return records

    def get_usage_records(self, scope, activity, activity_id):
        """Get log records for each usage of the activity given in the definitions."""
        records = []
        usage_list = []
        if activity in self.definitions["activity_descriptions"]:
            if "usage" in self.definitions["activity_descriptions"][activity]:
                usage_list = self.definitions["activity_descriptions"][activity]["usage"] or []
        self.usage_ids = []
        for item_description in usage_list:
            props = self.get_item_properties(scope, item_description)
            if "id" in props:
                entity_id = props.pop("id")
                if "namespace" in props:
                    entity_id = props.pop("namespace") + ":" + entity_id
                # Usage record
                self.usage_ids.append(entity_id)
                prov_record = {
                    "activity_id": activity_id,
                    "used_id": entity_id,
                }
                if "role" in item_description:
                    prov_record.update({"used_role": item_description["role"]})
                # Entity record (if not just the id)
                prov_record_ent = {}
                if "entity_description" in item_description:
                    prov_record_ent.update({"entity_description": item_description["entity_description"]})
                if "value" in item_description:
                    prov_record_ent.update({"location": item_description["value"]})
                for prop in props:
                    prov_record_ent.update({prop: props[prop]})
                if prov_record_ent:
                    prov_record_ent.update({entity_id: entity_id})
                    records.append(prov_record_ent)
                records.append(prov_record)
        return records

    def log_generation(self, scope, activity, activity_id, result=None):
        """Log generated entities."""
        generation_list = []
        if activity in self.definitions["activity_descriptions"]:
            generation_list = self.definitions["activity_descriptions"][activity]["generation"] or []
        returned_entity_id = None
        returned_entity_modifier = 0
        var_name = ""
        log_returned_entity = False
        if self.config['log_returned_result'] and result:
            entity_id = self.get_entity_id(result, {})
            # Check if entity is in traced_variables
            # entity_id, modifier, var_name = self.check_traced_variable(entity_id)
            modifier = 0
            for var, tv_dict in self.traced_variables.items():
                previous_ids = tv_dict["previous_ids"]
                modifier = 0
                if entity_id in previous_ids:
                    var_name = var
                    # Update modifier to get an id that has not yet been generated
                    while entity_id in previous_ids:
                        modifier += 1
                        entity_id += 1
                    self.logger.warning(f'id has already been taken by a variable'
                                        f' ({var} {entity_id}): '
                                        f'update modifier to {modifier}')
                    previous_ids.append(entity_id)
                    self.traced_variables[var] = {
                        "last_id": entity_id,
                        "previous_ids": previous_ids,
                        "item_description": tv_dict["item_description"],
                        "modifier": modifier,
                    }
            returned_entity_id = entity_id
            returned_entity_modifier = modifier
            log_returned_entity = True  # changed to False later if returned entity is already logged
            if not var_name:
                # entity_id was not found in traced_variables, need to add it as _returned_result
                if "_returned_result" in self.traced_variables:
                    tv_dict = self.traced_variables["_returned_result"]
                    tv_dict["last_id"] = returned_entity_id
                    tv_dict["previous_ids"].append(returned_entity_id)
                    tv_dict["modifier"] = 0
                else:
                    self.traced_variables["_returned_result"] = {
                        "last_id": returned_entity_id,
                        "previous_ids": [returned_entity_id],
                        "item_description": {},
                        "modifier": 0,
                    }
        for item_description in generation_list:
            props = self.get_item_properties(scope, item_description)
            if "id" in props:
                entity_id = props.pop("id")
                # Keep new entity as traced
                # entity_id, modifier = self.keep_as_traced_variable(entity_id, var=var)
                modifier = 0
                if "value" in item_description:
                    var = item_description["value"]
                    if var in self.traced_variables:
                        tv_dict = self.traced_variables[var]
                        previous_ids = tv_dict["previous_ids"]
                        entity_id -= tv_dict["modifier"]
                        modifier = 0  # tv_dict["modifier"]  # try first to generate without modifier
                        while entity_id in previous_ids:
                            modifier += 1
                            entity_id += 1
                            self.logger.warning(f'id has already been taken by this variable '
                                                f'({item_description["value"]} {entity_id}): '
                                                f'update modifier to {modifier}')
                        previous_ids.append(entity_id)
                    else:
                        modifier = 0
                        previous_ids = [entity_id]
                    self.traced_variables[item_description["value"]] = {
                        "last_id": entity_id,
                        "previous_ids": previous_ids,
                        "item_description": item_description,
                        "modifier": modifier,
                    }
                if entity_id == returned_entity_id:
                    # returned entity is already logged from the definition
                    log_returned_entity = False
                if "namespace" in props:
                    entity_id = props.pop("namespace") + ":" + entity_id
                # Generation record
                prov_record = {
                    "activity_id": activity_id,
                    "generated_id": entity_id,
                }
                if "role" in item_description:
                    prov_record.update({"generated_role": item_description["role"]})
                # Entity record
                prov_record_ent = {
                    "entity_id": entity_id,
                }
                if "entity_description" in item_description:
                    prov_record_ent.update({"entity_description": item_description["entity_description"]})
                if "value" in item_description:
                    prov_record_ent.update({"location": item_description["value"]})
                if modifier:
                    prov_record_ent.update({"modifier": modifier})
                for prop in props:
                    prov_record_ent.update({prop: props[prop]})
                self.log_prov_record(prov_record_ent)
                self.log_prov_record(prov_record)
                if "has_members" in item_description:
                    self.log_members(entity_id, item_description["has_members"], scope)
                if "has_progenitors" in item_description:
                    self.log_progenitors(entity_id, item_description["has_progenitors"], scope)
        if log_returned_entity:
            # The detected returned entity was not yet logged, add a generic generation
            # Generation record
            prov_record = {
                "activity_id": activity_id,
                "generated_id": returned_entity_id,
                "generated_role": "result",
            }
            # Entity record
            prov_record_ent = {
                "entity_id": returned_entity_id,
            }
            if var_name:
                prov_record_ent.update({"location": var_name})
            if modifier:
                prov_record_ent.update({"modifier": returned_entity_modifier})
            self.log_prov_record(prov_record_ent)
            self.log_prov_record(prov_record)

    def log_members(self, entity_id, subitem, scope):
        """Log members of and entity."""
        if "list" in subitem:
            member_list = self.get_nested_value(scope, subitem["list"]) or []
        else:
            member_list = [scope]
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
                if "entity_description" in subitem:
                    prov_record_ent.update({"entity_description": subitem["entity_description"]})
                for prop in props:
                    prov_record_ent.update({prop: props[prop]})
                self.log_prov_record(prov_record_ent)
                self.log_prov_record(prov_record)

    def log_progenitors(self, entity_id, subitem, scope):
        """Log progenitors of and entity."""
        if "list" in subitem:
            progenitor_list = self.get_nested_value(scope, subitem["list"]) or []
        else:
            progenitor_list = [scope]
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

    def log_file_generation(self, file_path, entity_description="", used=None, role="", activity_name=""):
        # get file properties
        if os.path.isfile(file_path):
            item_description = dict(
                file_path=file_path,
                entity_description=entity_description,
            )
            entity_id = self.get_entity_id(file_path, item_description)
            prov_record = {
                "entity_id": entity_id,
                "entity_description": entity_description,
                "location": file_path,
                "hash": entity_id,
                "hash_type": self.config["hash_type"],
            }
            self.log_prov_record(prov_record)
            if activity_name:
                activity_id = self.gen_activity_id()
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
