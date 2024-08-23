from collections import deque
from functools import lru_cache
from dcicutils.function_cache_decorator import function_cache
import io
import json
from pyramid.config import Configurator as PyramidConfigurator
from pyramid.paster import get_app as pyramid_get_app
from pyramid.response import Response as PyramidResponse
from pyramid.router import Router as PyramidRouter
import os
import re
import requests
from requests.models import Response
from threading import Thread
from typing import Callable, Dict, List, Optional, Tuple, Type, Union
from uuid import uuid4 as uuid
from webtest.app import TestApp, TestResponse
from wsgiref.simple_server import make_server as wsgi_make_server
from dcicutils.common import APP_SMAHT, OrchestratedApp, ORCHESTRATED_APPS
from dcicutils.ff_utils import get_metadata, get_schema, patch_metadata, post_metadata
from dcicutils.misc_utils import to_camel_case, VirtualApp
from dcicutils.schema_utils import get_identifying_properties
from dcicutils.tmpfile_utils import temporary_file

Portal = Type["Portal"]  # Forward type reference for type hints.
OptionalResponse = Optional[Union[Response, TestResponse]]


class Portal:
    """
    This is meant to be an Über wrapper for Portal access. It can be created in a variety of ways:
    1. From a (Portal) .ini file (e.g. development.ini).
    2. From a key dictionary, containing "key" and "secret" and (optional) "server" property values.
    3. From a key pair tuple, containing (in order) a key and secret values.
    4. From a keys .json file residing in ~/.{app}-keys.json where the given "app" value is either "smaht", "cgap",
       or "fourfront"; where is assumed to contain a dictionary with a key for the given "env" value, e.g. smaht-local;
       and with a dictionary value containing "key" and "secret" property values, and an optional "server" property;
       if an "app" value is not specified but the given "env" value begins with one of the app values then that value
       will be used, i.e. e.g. if "env" is "smaht-local" and app is unspecified than app is assumed to be "smaht".
    5. From a keys .json file as described above (#4) but rather than be identified by the given "env" value it
       is looked up via the given "server" name and the "server" key dictionary value in the key file.
    6. From a full path to a keys .json file.
    7. From a given "vapp" value; which may be a webtest.app.TestApp,
       or a dcicutils.misc_utils.VirtualApp, or even a pyramid.router.Router.
    8. From another Portal object (i.e. copy constructor).
    """
    DEFAULT_APP = APP_SMAHT
    KEYS_FILE_DIRECTORY = "~"
    MIME_TYPE_JSON = "application/json"
    FILE_TYPE_SCHEMA_NAME = "File"

    # Object lookup strategies; on a per-reference (type/value) basis, used currently ONLY by
    # structured_data.py; controlled by an optional lookup_strategy callable; default is
    # lookup at root path but after the specified type path lookup, and then lookup all subtypes;
    # can choose to lookup root path first, or not lookup root path at all, or not lookup
    # subtypes at all; the lookup_strategy callable if specified should take a type_name
    # and value (string) arguements and return an integer of any of the below ORed together.
    # The main purpose of this is optimization; to minimize portal lookups; since for example,
    # currently at least, /{type}/{accession} does not work but /{accession} does; so we
    # currently (smaht-portal/.../ingestion_processors) use LOOKUP_ROOT_FIRST for this.
    # And current usage NEVER has LOOKUP_SUBTYPES turned OFF; but support just in case.
    LOOKUP_UNDEFINED = 0
    LOOKUP_SPECIFIED_TYPE = 0x0001
    LOOKUP_ROOT = 0x0002
    LOOKUP_ROOT_FIRST = 0x0004 | LOOKUP_ROOT
    LOOKUP_SUBTYPES = 0x0008
    LOOKUP_DEFAULT = LOOKUP_SPECIFIED_TYPE | LOOKUP_ROOT | LOOKUP_SUBTYPES

    def __init__(self,
                 arg: Optional[Union[Portal, TestApp, VirtualApp, PyramidRouter, dict, tuple, str]] = None,
                 env: Optional[str] = None, server: Optional[str] = None,
                 app: Optional[OrchestratedApp] = None,
                 raise_exception: bool = True) -> None:

        def init(unspecified: Optional[list] = []) -> None:
            self._ini_file = None
            self._key = None
            self._keys_file = None
            self._env = None
            self._server = None
            self._vapp = None
            for arg in unspecified:
                if arg is not None:
                    raise Exception("Portal initialization error; extraneous arguments.")

        def init_from_portal(portal: Portal, unspecified: Optional[list] = None) -> None:
            init(unspecified=unspecified)
            self._ini_file = portal._ini_file
            self._key = portal._key
            self._keys_file = portal._keys_file
            self._env = portal._env
            self._server = portal._server
            self._vapp = portal._vapp
            self._app = portal._app

        def init_from_vapp(vapp: Union[TestApp, VirtualApp, PyramidRouter], unspecified: Optional[list] = []) -> None:
            init(unspecified=unspecified)
            self._vapp = Portal._create_vapp(vapp)

        def init_from_ini_file(ini_file: str, unspecified: Optional[list] = []) -> None:
            init(unspecified=unspecified)
            self._ini_file = ini_file
            self._vapp = Portal._create_vapp(ini_file)

        def init_from_key(key: dict, server: Optional[str], unspecified: Optional[list] = []) -> None:
            init(unspecified=unspecified)
            if (isinstance(key_id := key.get("key"), str) and key_id and
                isinstance(secret := key.get("secret"), str) and secret):  # noqa
                self._key = {"key": key_id, "secret": secret}
                if (isinstance(server, str) and server) or (isinstance(server := key.get("server"), str) and server):
                    if server := Portal._normalize_server(server):
                        if isinstance(key_server := key.get("server"), str) and key_server:
                            if Portal._normalize_server(key_server) != server:
                                raise Exception(f"Portal server inconsistency: {server} vs {key_server}")
                        self._key["server"] = self._server = server
            if not self._key:
                raise Exception("Portal initialization error; from key.")

        def init_from_key_pair(key_pair: tuple, server: Optional[str], unspecified: Optional[list] = []) -> None:
            if len(key_pair) >= 2:
                init_from_key({"key": key_pair[0], "secret": key_pair[1]}, server, unspecified=unspecified)
            else:
                raise Exception("Portal initialization error; from key-pair.")

        def init_from_keys_file(keys_file: str, env: Optional[str], server: Optional[str],
                                unspecified: Optional[list] = []) -> None:
            key, env = Portal._lookup_in_keys_file(keys_file, env, server, raise_exception=True)
            if key:
                init_from_key(key, server)
                self._keys_file = keys_file
                self._env = env

        def init_from_env_server_app(env: str, server: str, app: Optional[str],
                                     unspecified: Optional[list] = None) -> None:
            if keys_file := Portal._default_keys_file(app, env, server):
                init_from_keys_file(keys_file, env, server, unspecified=unspecified)
            else:
                init(unspecified=unspecified)
                self._env = env
                self._server = server

        if (valid_app := app) and not (valid_app := Portal._valid_app(app)):
            raise Exception(f"Portal initialization error; invalid app: {app}")
        self._app = valid_app
        if isinstance(arg, Portal):
            init_from_portal(arg, unspecified=[env, server, app])
        elif isinstance(arg, (TestApp, VirtualApp, PyramidRouter)):
            init_from_vapp(arg, unspecified=[env, server, app])
        elif isinstance(arg, str) and arg.endswith(".ini"):
            init_from_ini_file(arg, unspecified=[env, server])
        elif isinstance(arg, dict):
            init_from_key(arg, server, unspecified=[env])
        elif isinstance(arg, tuple):
            init_from_key_pair(arg, server, unspecified=[env])
        elif isinstance(arg, str) and arg.endswith(".json"):
            init_from_keys_file(arg, env, server)
        elif isinstance(arg, str) and arg:
            init_from_env_server_app(arg, server, app, unspecified=[env])
        elif (isinstance(env, str) and env) or (isinstance(server, str) and server):
            init_from_env_server_app(env, server, app, unspecified=[arg])
        elif not arg and (keys_file := Portal._default_keys_file(self._app or Portal.DEFAULT_APP, env, server)):
            # If no initial arg then look for default app keys file.
            init_from_keys_file(keys_file, env, server)
        elif raise_exception:
            raise Exception("Portal initialization error; insufficient arguments.")
        else:
            init()
        if not self.vapp and not self.key and raise_exception:
            raise Exception("Portal initialization error; neither key nor vapp defined.")

    @property
    def ini_file(self) -> Optional[str]:
        return self._ini_file

    @property
    def key(self) -> Optional[dict]:
        return self._key

    @property
    def key_pair(self) -> Optional[Tuple[str, str]]:
        return (key.get("key"), key.get("secret")) if (key := self.key) else None

    @property
    def key_id(self) -> Optional[str]:
        return key.get("key") if (key := self.key) else None

    @property
    def secret(self) -> Optional[str]:
        return key.get("secret") if (key := self.key) else None

    @property
    def keys_file(self) -> Optional[str]:
        return self._keys_file

    @property
    def env(self) -> Optional[str]:
        return self._env

    @property
    def server(self) -> Optional[str]:
        return key.get("server") if (key := self.key) and key.get("server") else self._server

    @property
    def app(self) -> Optional[str]:
        return self._app

    @property
    def vapp(self) -> Optional[TestApp]:
        return self._vapp

    def get(self, url: str, follow: bool = True,
            raw: bool = False, database: bool = False, raise_for_status: bool = False, **kwargs) -> OptionalResponse:
        url = self.url(url, raw, database)
        if not self.vapp:
            response = requests.get(url, allow_redirects=follow, **self._kwargs(**kwargs))
        else:
            response = self.vapp.get(url, **self._kwargs(**kwargs))
            if response and response.status_code in [301, 302, 303, 307, 308] and follow:
                response = response.follow()
            response = self._response(response)
        if raise_for_status:
            response.raise_for_status()
        return response

    def patch(self, url: str, data: Optional[dict] = None, json: Optional[dict] = None,
              raise_for_status: bool = False, **kwargs) -> OptionalResponse:
        url = self.url(url)
        if not self.vapp:
            response = requests.patch(url, data=data, json=json, **self._kwargs(**kwargs))
        else:
            response = self.vapp.patch_json(url, json or data, **self._kwargs(**kwargs))
            response = self._response(response)
        if raise_for_status:
            response.raise_for_status()
        return response

    def post(self, url: str, data: Optional[dict] = None, json: Optional[dict] = None, files: Optional[dict] = None,
             raise_for_status: bool = False, **kwargs) -> OptionalResponse:
        url = self.url(url)
        if files and not ("headers" in kwargs):
            # Setting headers to None when using files implies content-type multipart/form-data.
            kwargs["headers"] = None
        if not self.vapp:
            response = requests.post(url, data=data, json=json, files=files, **self._kwargs(**kwargs))
        else:
            if files:
                response = self.vapp.post(url, json or data, upload_files=files, **self._kwargs(**kwargs))
            else:
                response = self.vapp.post_json(url, json or data, upload_files=files, **self._kwargs(**kwargs))
            response = self._response(response)
        if raise_for_status:
            response.raise_for_status()
        return response

    def get_metadata(self, object_id: str, raw: bool = False,
                     database: bool = False, raise_exception: bool = True) -> Optional[dict]:
        if isinstance(raw, bool) and raw:
            add_on = "frame=raw" + ("&datastore=database" if isinstance(database, bool) and database else "")
        elif database:
            add_on = "datastore=database"
        else:
            add_on = ""
        if raise_exception:
            return get_metadata(obj_id=object_id, vapp=self.vapp, key=self.key, add_on=add_on)
        else:
            try:
                return get_metadata(obj_id=object_id, vapp=self.vapp, key=self.key, add_on=add_on)
            except Exception:
                return None

    def patch_metadata(self, object_id: str, data: dict, check_only: bool = False) -> Optional[dict]:
        if self.key:
            return patch_metadata(obj_id=object_id, patch_item=data, key=self.key,
                                  add_on="check_only=True" if check_only else "")
        return self.patch(f"/{object_id}{'?check_only=True' if check_only else ''}", data).json()

    def post_metadata(self, object_type: str, data: dict, check_only: bool = False) -> Optional[dict]:
        if self.key:
            return post_metadata(schema_name=object_type, post_item=data, key=self.key,
                                 add_on="check_only=True" if check_only else "")
        return self.post(f"/{object_type}{'?check_only=True' if check_only else ''}", data).json()

    def head(self, url: str, follow: bool = True, raise_exception: bool = False, **kwargs) -> Optional[int]:
        try:
            response = requests.head(self.url(url), **self._kwargs(**kwargs))
            if response and response.status_code in [301, 302, 303, 307, 308] and (follow is not False):
                response = response.follow()
            return response.status_code
        except Exception as e:
            if raise_exception is True:
                raise e
        return None

    def get_health(self) -> OptionalResponse:
        return self.get("/health")

    def ping(self) -> bool:
        try:
            return self.get_health().status_code == 200
        except Exception:
            return False

    @lru_cache(maxsize=100)
    def get_schema(self, schema_name: str) -> Optional[dict]:
        try:
            return get_schema(self.schema_name(schema_name), portal_vapp=self.vapp, key=self.key)
        except Exception:
            return None

    @lru_cache(maxsize=1)
    def get_schemas(self) -> dict:
        return self.get("/profiles/").json()

    @staticmethod
    @lru_cache(maxsize=100)
    def schema_name(name: str) -> str:
        name = os.path.basename(name).replace(" ", "") if isinstance(name, str) else ""
        if (dot := name.rfind(".")) > 0:
            name = name[0:dot]
        return to_camel_case(name)
        # return to_camel_case(name.replace(" ", "") if not name.endswith(".json") else name[:-5])

    def is_schema_type(self, schema_name_or_portal_object: Union[str, dict], target_schema_name: str,
                       _schemas_super_type_map: Optional[list] = None) -> bool:
        """
        If the given (first) schema_name_or_portal_object argument is a string then returns True iff
        the given schema (type) name isa type of the given target schema (type) name, i.e. the given
        schema type is the given target schema type or has an ancestor which is that type.
        If the given (first) schema_name_or_portal_object argument is a dictionary then
        returns True iff this object value isa type of the given target schema type.
        """
        if isinstance(schema_name_or_portal_object, dict):
            return self.isinstance_schema(schema_name_or_portal_object, target_schema_name)
        elif not isinstance(schema_name_or_portal_object, str) or not schema_name_or_portal_object:
            return False
        schema_name = self.schema_name(schema_name_or_portal_object).lower()
        target_schema_name = self.schema_name(target_schema_name).lower()
        if schema_name == target_schema_name:
            return True
        if super_type_map := (_schemas_super_type_map or self.get_schemas_super_type_map()):
            for super_type in super_type_map:
                if super_type.lower() == target_schema_name:
                    for value in super_type_map[super_type]:
                        if value.lower() == schema_name:
                            return True
        return False

    def is_schema_file_type(self, schema_name_or_portal_object: Union[str, dict]) -> bool:
        return self.is_schema_type(schema_name_or_portal_object, self.FILE_TYPE_SCHEMA_NAME)

    def isinstance_schema(self, portal_object: dict, target_schema_name: str) -> bool:
        """
        Returns True iff the given object isa type of the given schema type.
        """
        if value_types := self.get_schema_types(portal_object):
            schemas_super_type_map = self.get_schemas_super_type_map()
            for value_type in value_types:
                if self.is_schema_type(value_type, target_schema_name, schemas_super_type_map):
                    return True
        return False

    @staticmethod
    def get_schema_types(portal_object: dict) -> Optional[List[str]]:
        if isinstance(portal_object, dict):
            if isinstance(value_types := portal_object.get("@type"), str):
                value_types = [value_types] if value_types else []
            elif not isinstance(value_types, list):
                value_types = []
            if isinstance(data_type := portal_object.get("data_type"), list):
                value_types.extend(data_type)
            elif isinstance(data_type, str):
                value_types.append(data_type)
            return value_types if value_types else None

    @staticmethod
    def get_schema_type(portal_object: dict) -> Optional[str]:
        if value_types := Portal.get_schema_types(portal_object):
            return value_types[0]

    @lru_cache(maxsize=1)
    def get_schemas_super_type_map(self) -> dict:
        """
        Returns the "super type map" for all of the known schemas (via /profiles).
        This is a dictionary with property names which are all known schema type names which
        have (one or more) sub-types, and the value of each such property name is an array
        of all of those sub-type names (direct and all descendents), in breadth first order.
        """
        def list_breadth_first(super_type_map: dict, super_type_name: str) -> dict:
            result = []
            queue = deque(super_type_map.get(super_type_name, []))
            while queue:
                result.append(subtype_name := queue.popleft())
                if subtype_name in super_type_map:
                    queue.extend(super_type_map[subtype_name])
            return result
        if not (schemas := self.get_schemas()):
            return {}
        super_type_map = {}
        for type_name in schemas:
            if isinstance(schema_type := schemas[type_name], dict):
                if super_type_name := schema_type.get("rdfs:subClassOf"):
                    super_type_name = super_type_name.replace("/profiles/", "").replace(".json", "")
                    if super_type_name != "Item":
                        if not super_type_map.get(super_type_name):
                            super_type_map[super_type_name] = [type_name]
                        elif type_name not in super_type_map[super_type_name]:
                            super_type_map[super_type_name].append(type_name)
        super_type_map_flattened = {}
        for super_type_name in super_type_map:
            super_type_map_flattened[super_type_name] = list_breadth_first(super_type_map, super_type_name)
        return super_type_map_flattened

    @lru_cache(maxsize=100)
    def get_schema_subtype_names(self, type_name: str) -> List[str]:
        if not (schemas_super_type_map := self.get_schemas_super_type_map()):
            return []
        return schemas_super_type_map.get(type_name, [])

    @function_cache(maxsize=100, serialize_key=True)
    def get_identifying_paths(self, portal_object: dict, portal_type: Optional[Union[str, dict]] = None,
                              first_only: bool = False,
                              lookup_strategy: Optional[Union[Callable, bool]] = None) -> List[str]:
        """
        Returns the list of the identifying Portal (URL) paths for the given Portal object. Favors any uuid
        and identifier based paths and defavors aliases based paths (ala self.get_identifying_property_names);
        no other ordering defined. Returns an empty list if no identifying properties or otherwise not found.
        Note that this is a newer version of what was in portal_object_utils and just uses the ref_lookup_stratey
        module directly, as it no longer needs to be exposed (to smaht-portal/ingester and smaht-submitr) and so
        this is a first step toward internalizing it to structured_data/portal_utils/portal_object_utils usages.
        """
        def is_lookup_specified_type(lookup_options: int) -> bool:
            return (lookup_options & Portal.LOOKUP_SPECIFIED_TYPE) == Portal.LOOKUP_SPECIFIED_TYPE
        def is_lookup_root(lookup_options: int) -> bool:  # noqa
            return (lookup_options & Portal.LOOKUP_ROOT) == Portal.LOOKUP_ROOT
        def is_lookup_root_first(lookup_options: int) -> bool:  # noqa
            return (lookup_options & Portal.LOOKUP_ROOT_FIRST) == Portal.LOOKUP_ROOT_FIRST
        def is_lookup_subtypes(lookup_options: int) -> bool:  # noqa
            return (lookup_options & Portal.LOOKUP_SUBTYPES) == Portal.LOOKUP_SUBTYPES

        results = []
        if not isinstance(portal_object, dict):
            return results
        if not (isinstance(portal_type, str) and portal_type):
            if isinstance(portal_type, dict):
                # It appears that the given portal_type is an actual schema dictionary.
                portal_type = self.schema_name(portal_type.get("title"))
            if not (isinstance(portal_type, str) and portal_type):
                if not (portal_type := self.get_schema_type(portal_object)):
                    return results
        if not callable(lookup_strategy):
            lookup_strategy = None if lookup_strategy is False else Portal._lookup_strategy
        for identifying_property in self.get_identifying_property_names(portal_type):
            if not (identifying_value := portal_object.get(identifying_property)):
                continue
            # The get_identifying_property_names call above ensures uuid is first if it is in the object.
            # And also note that ALL schemas do in fact have identifyingProperties which do in fact have
            # uuid, except for a couple "Test" ones, and (for some reason) SubmittedItem; otherwise we
            # might have a special case to check the Portal object explicitly for uuid, but no need.
            if identifying_property == "uuid":
                #
                # Note this idiosyncrasy with Portal paths: the only way we do NOT get a (HTTP 301) redirect
                # is if we use the lower-case-dashed-plural based version of the path, e.g. all of these:
                #
                # - /d13d06c1-218e-4f61-aaf0-91f226248b3c
                # - /d13d06c1-218e-4f61-aaf0-91f226248b3c/
                # - /FileFormat/d13d06c1-218e-4f61-aaf0-91f226248b3c
                # - /FileFormat/d13d06c1-218e-4f61-aaf0-91f226248b3c/
                # - /files-formats/d13d06c1-218e-4f61-aaf0-91f226248b3c
                #
                # Will result in a (HTTP 301) redirect to:
                #
                # - /files-formats/d13d06c1-218e-4f61-aaf0-91f226248b3c/
                #
                # Unfortunately, this code here has no reasonable way of getting that lower-case-dashed-plural
                # based name (e.g. file-formats) from the schema/portal type name (e.g. FileFormat); as the
                # information is contained, for this example, in the snovault.collection decorator for the
                # endpoint definition in smaht-portal/.../types/file_format.py. Unfortunately merely because
                # behind-the-scenes an extra round-trip HTTP request will occur, but happens automatically.
                # And note the disction of just using /{uuid} here rather than /{type}/{uuid} as in the else
                # statement below is not really necessary; just here for emphasis that this is all that's needed.
                #
                # TODO
                # Consider (from PR-308) writing a portal API for retrieving possible path formats.
                #
                if first_only is True:
                    results.append(f"/{portal_type}/{identifying_value}")
                else:
                    results.append(f"/{identifying_value}")
            elif isinstance(identifying_value, list):
                for identifying_value_item in identifying_value:
                    if identifying_value_item:
                        results.append(f"/{portal_type}/{identifying_value_item}")
            else:
                lookup_options = Portal.LOOKUP_UNDEFINED
                if schema := self.get_schema(portal_type):
                    if callable(lookup_strategy):
                        lookup_options, validator = lookup_strategy(self, portal_type, schema, identifying_value)
                        if callable(validator):
                            if validator(schema, identifying_property, identifying_value) is False:
                                continue
                    if pattern := schema.get("properties", {}).get(identifying_property, {}).get("pattern"):
                        if not re.match(pattern, identifying_value):
                            # If this identifying value is for a (identifying) property which has a
                            # pattern, and the value does NOT match the pattern, then do NOT include
                            # this value as an identifying path, since it cannot possibly be found.
                            continue
                if lookup_options == Portal.LOOKUP_UNDEFINED:
                    lookup_options = Portal.LOOKUP_DEFAULT
                if is_lookup_root_first(lookup_options):
                    results.append(f"/{identifying_value}")
                if is_lookup_specified_type(lookup_options) and portal_type:
                    results.append(f"/{portal_type}/{identifying_value}")
                if is_lookup_root(lookup_options) and not is_lookup_root_first(lookup_options):
                    results.append(f"/{identifying_value}")
                if is_lookup_subtypes(lookup_options):
                    for subtype_name in self.get_schema_subtype_names(portal_type):
                        results.append(f"/{subtype_name}/{identifying_value}")
            if (first_only is True) and results:
                return results
        return results

    @function_cache(maxsize=100, serialize_key=True)
    def get_identifying_path(self, portal_object: dict, portal_type: Optional[Union[str, dict]] = None,
                             lookup_strategy: Optional[Union[Callable, bool]] = None) -> Optional[str]:
        if identifying_paths := self.get_identifying_paths(portal_object, portal_type, first_only=True,
                                                           lookup_strategy=lookup_strategy):
            return identifying_paths[0]
        return None

    @function_cache(maxsize=100, serialize_key=True)
    def get_identifying_property_names(self, schema: Union[str, dict],
                                       portal_object: Optional[dict] = None) -> List[str]:
        """
        Returns the list of identifying property names for the given Portal schema, which may be
        either a schema name or a schema object. If a Portal object is also given then restricts this
        set of identifying properties to those which actually have values within this Portal object.
        Favors the uuid and identifier property names and defavors the aliases property name; no other
        ordering imposed. Returns empty list if no identifying properties or otherwise not found.
        """
        results = []
        if isinstance(schema, str):
            if not (schema := self.get_schema(schema)):
                return results
        elif not isinstance(schema, dict):
            return results
        if not (identifying_properties := get_identifying_properties(schema)):
            return results
        identifying_properties = list(set(identifying_properties))  # paranoid dedup
        identifying_properties = [*identifying_properties]  # copy so as not to change schema if given
        favored_identifying_properties = ["uuid", "identifier"]
        defavored_identifying_properties = ["aliases"]
        for favored_identifying_property in reversed(favored_identifying_properties):
            if favored_identifying_property in identifying_properties:
                identifying_properties.remove(favored_identifying_property)
                identifying_properties.insert(0, favored_identifying_property)
        for defavored_identifying_property in defavored_identifying_properties:
            if defavored_identifying_property in identifying_properties:
                identifying_properties.remove(defavored_identifying_property)
                identifying_properties.append(defavored_identifying_property)
        if isinstance(portal_object, dict):
            for identifying_property in [*identifying_properties]:
                if portal_object.get(identifying_property) is None:
                    identifying_properties.remove(identifying_property)
        return identifying_properties

    @staticmethod
    def _lookup_strategy(portal: Portal, type_name: str, schema: dict, value: str) -> (int, Optional[str]):
        #
        # Note this slightly odd situation WRT object lookups by submitted_id and accession:
        # -----------------------------+-----------------------------------------------+---------------+
        # PATH                         | EXAMPLE                                       | LOOKUP RESULT |
        # -----------------------------+-----------------------------------------------+---------------+
        # /submitted_id                | //UW_FILE-SET_COLO-829BL_HI-C_1               | NOT FOUND     |
        # /UnalignedReads/submitted_id | /UnalignedReads/UW_FILE-SET_COLO-829BL_HI-C_1 | FOUND         |
        # /SubmittedFile/submitted_id  | /SubmittedFile/UW_FILE-SET_COLO-829BL_HI-C_1  | FOUND         |
        # /File/submitted_id           | /File/UW_FILE-SET_COLO-829BL_HI-C_1           | NOT FOUND     |
        # -----------------------------+-----------------------------------------------+---------------+
        # /accession                   | /SMAFSFXF1RO4                                 | FOUND         |
        # /UnalignedReads/accession    | /UnalignedReads/SMAFSFXF1RO4                  | NOT FOUND     |
        # /SubmittedFile/accession     | /SubmittedFile/SMAFSFXF1RO4                   | NOT FOUND     |
        # /File/accession              | /File/SMAFSFXF1RO4                            | FOUND         |
        # -----------------------------+-----------------------------------------------+---------------+
        #
        def ref_validator(schema: Optional[dict],
                          property_name: Optional[str], property_value: Optional[str]) -> Optional[bool]:
            """
            Returns False iff objects of type represented by the given schema, CANNOT be referenced with
            a Portal path using the given property name and its given property value, otherwise returns None.

            For example, if the schema is for UnalignedReads and the property name is accession, then we will
            return False iff the given property value is NOT a properly formatted accession ID; otherwise, we
            will return None, which indicates that the caller (e.g. dcicutils.structured_data.Portal.ref_exists)
            will continue executing its default behavior, which is to check other ways in which the given type
            CANNOT be referenced by the given value, i.e. it checks other identifying properties for the type
            and makes sure any patterns (e.g. for submitted_id or uuid) are ahered to.

            The goal (in structured_data) being to detect if a type is being referenced in such a way that
            CANNOT possibly be allowed, i.e. because none of its identifying types are in the required form,
            if indeed there any requirements. It is assumed/guaranteed the given property name is indeed an
            identifying property for the given type.
            """
            if property_format := schema.get("properties", {}).get(property_name, {}).get("format"):
                if (property_format == "accession") and (property_name == "accession"):
                    if not Portal._is_accession_id(property_value):
                        return False
            return None

        DEFAULT_RESULT = (Portal.LOOKUP_DEFAULT, ref_validator)
        if not value:
            return DEFAULT_RESULT
        if not schema:
            if not isinstance(portal, Portal) or not (schema := portal.get_schema(type_name)):
                return DEFAULT_RESULT
        if schema_properties := schema.get("properties"):
            if schema_properties.get("accession") and Portal._is_accession_id(value):
                # Case: lookup by accession (only by root).
                return (Portal.LOOKUP_ROOT, ref_validator)
            elif schema_property_info_submitted_id := schema_properties.get("submitted_id"):
                if schema_property_pattern_submitted_id := schema_property_info_submitted_id.get("pattern"):
                    if re.match(schema_property_pattern_submitted_id, value):
                        # Case: lookup by submitted_id (only by specified type).
                        return (Portal.LOOKUP_SPECIFIED_TYPE, ref_validator)
        return DEFAULT_RESULT

    @staticmethod
    def _is_accession_id(value: str) -> bool:
        # This is here for now because of problems with circular dependencies.
        # See: smaht-portal/.../schema_formats.py/is_accession(instance) ...
        return isinstance(value, str) and re.match(r"^SMA[1-9A-Z]{9}$", value) is not None

    def url(self, url: str, raw: bool = False, database: bool = False) -> str:
        if not isinstance(url, str) or not url:
            return "/"
        elif (lowercase_url := url.lower()).startswith("http://") or lowercase_url.startswith("https://"):
            return url
        elif not (url := re.sub(r"/+", "/", url)).startswith("/"):
            url = "/"
        url = self.server + url if self.server else url
        if isinstance(raw, bool) and raw:
            url += ("&" if "?" in url else "?") + "frame=raw"
        if isinstance(database, bool) and database:
            url += ("&" if "?" in url else "?") + "datastore=database"
        return url

    def _kwargs(self, **kwargs) -> dict:
        if "headers" in kwargs:
            result_kwargs = {"headers": kwargs["headers"]}
        else:
            result_kwargs = {"headers": {"Content-type": Portal.MIME_TYPE_JSON, "Accept": Portal.MIME_TYPE_JSON}}
        if self.key_pair:
            result_kwargs["auth"] = self.key_pair
        if isinstance(timeout := kwargs.get("timeout"), int):
            result_kwargs["timeout"] = timeout
        return result_kwargs

    @staticmethod
    def _default_keys_file(app: Optional[str], env: Optional[str], server: Optional[str]) -> Optional[str]:
        def infer_app_from_env(env: str) -> Optional[str]:  # noqa
            if isinstance(env, str) and (lowercase_env := env.lower()):
                if app := [app for app in ORCHESTRATED_APPS if lowercase_env.startswith(app.lower())]:
                    return Portal._valid_app(app[0])
        if (app := Portal._valid_app(app)) or (app := infer_app_from_env(env)):
            keys_file = os.path.expanduser(os.path.join(Portal.KEYS_FILE_DIRECTORY, f".{app.lower()}-keys.json"))
            return keys_file if os.path.exists(keys_file) else None
        if not app:
            for app in ORCHESTRATED_APPS:
                if keys_file := Portal._default_keys_file(app, env, server):
                    if Portal._lookup_in_keys_file(keys_file, env, server)[0]:
                        return keys_file

    @staticmethod
    def _lookup_in_keys_file(keys_file: str, env: Optional[str], server: Optional[str],
                             raise_exception: bool = False) -> Tuple[Optional[dict], Optional[str]]:
        try:
            with io.open(keys_file := os.path.expanduser(keys_file)) as f:
                keys = json.load(f)
        except Exception:
            if raise_exception:
                raise Exception(f"Portal initialization error; cannot open keys-file: {keys_file}")
            return None, None
        if isinstance(env, str) and env and isinstance(key := keys.get(env), dict):
            return key, env
        elif (isinstance(server, str) and (server := Portal._normalize_server(server)) and
              (key := [keys[k] for k in keys if Portal._normalize_server(keys[k].get("server")) == server])):
            return key[0], env
        elif not env and len(keys) == 1 and (env := next(iter(keys))) and isinstance(key := keys[env], dict):
            return key, env
        else:
            if raise_exception:
                raise Exception(f"Portal initialization error;"
                                f" {env or server or None} not found in keys-file: {keys_file}")
            return None, None

    @staticmethod
    def _normalize_server(server: str) -> Optional[str]:
        prefix = ""
        if (lowercase_server := server.lower()).startswith("http://"):
            prefix = "http://"
        elif lowercase_server.startswith("https://"):
            prefix = "https://"
        if prefix:
            if (server := re.sub(r"/+", "/", server[len(prefix):])).startswith("/"):
                server = server[1:]
            if len(server) > 1 and server.endswith("/"):
                server = server[:-1]
            return prefix + server if server else None

    @staticmethod
    def _valid_app(app: Optional[str]) -> Optional[str]:
        if isinstance(app, str) and (app_lowercase := app.lower()):
            for value in ORCHESTRATED_APPS:
                if value.lower() == app_lowercase:
                    return value

    def _response(self, response: TestResponse) -> TestResponse:
        if response and isinstance(getattr(response.__class__, "json"), property):
            class TestResponseWrapper(TestResponse):  # For consistency change json property to method.
                def __init__(self, response, **kwargs):
                    super().__init__(**kwargs)
                    self._response = response
                def __getattr__(self, attr):  # noqa
                    return getattr(self._response, attr)
                def json(self):  # noqa
                    return self._response.json
                def raise_for_status(self):  # noqa
                    if self.status_code < 200 or self.status_code > 399:
                        raise requests.exceptions.HTTPError(f"HTTP Error: {self.status_code}", response=self)
            response = TestResponseWrapper(response)
        return response

    @staticmethod
    def _create_vapp(arg: Union[TestApp, VirtualApp, PyramidRouter, str] = None) -> TestApp:
        if isinstance(arg, TestApp):
            return arg
        elif isinstance(arg, VirtualApp):
            if not isinstance(arg.wrapped_app, TestApp):
                raise Exception("Portal._create_vapp VirtualApp argument error.")
            return arg.wrapped_app
        if isinstance(arg, PyramidRouter):
            router = arg
        elif isinstance(arg, str) or not arg:
            router = pyramid_get_app(arg or "development.ini", "app")
        else:
            raise Exception("Portal._create_vapp argument error.")
        return TestApp(router, {"HTTP_ACCEPT": Portal.MIME_TYPE_JSON, "REMOTE_USER": "TEST"})

    @staticmethod
    def create_for_testing(arg: Optional[Union[str, bool, List[dict], dict, Callable]] = None) -> Portal:
        if isinstance(arg, list) or isinstance(arg, dict) or isinstance(arg, Callable):
            return Portal(Portal._create_router_for_testing(arg))
        elif isinstance(arg, str) and arg.endswith(".ini"):
            return Portal(arg)
        elif arg == "local" or arg is True:
            minimal_ini_for_testing = "\n".join([
                "[app:app]\nuse = egg:encoded\nfile_upload_bucket = dummy",
                "sqlalchemy.url = postgresql://postgres@localhost:5441/postgres?host=/tmp/snovault/pgdata",
                "multiauth.groupfinder = encoded.authorization.smaht_groupfinder",
                "multiauth.policies = auth0 session remoteuser accesskey",
                "multiauth.policy.session.namespace = mailto",
                "multiauth.policy.session.use = encoded.authentication.NamespacedAuthenticationPolicy",
                "multiauth.policy.session.base = pyramid.authentication.SessionAuthenticationPolicy",
                "multiauth.policy.remoteuser.namespace = remoteuser",
                "multiauth.policy.remoteuser.use = encoded.authentication.NamespacedAuthenticationPolicy",
                "multiauth.policy.remoteuser.base = pyramid.authentication.RemoteUserAuthenticationPolicy",
                "multiauth.policy.accesskey.namespace = accesskey",
                "multiauth.policy.accesskey.use = encoded.authentication.NamespacedAuthenticationPolicy",
                "multiauth.policy.accesskey.base = encoded.authentication.BasicAuthAuthenticationPolicy",
                "multiauth.policy.accesskey.check = encoded.authentication.basic_auth_check",
                "multiauth.policy.auth0.use = encoded.authentication.NamespacedAuthenticationPolicy",
                "multiauth.policy.auth0.namespace = auth0",
                "multiauth.policy.auth0.base = encoded.authentication.Auth0AuthenticationPolicy"
            ])
        else:
            minimal_ini_for_testing = "[app:app]\nuse = egg:encoded\nsqlalchemy.url = postgresql://dummy\n"
        with temporary_file(content=minimal_ini_for_testing, suffix=".ini") as ini_file:
            return Portal(ini_file)

    @staticmethod
    def _create_router_for_testing(endpoints: Optional[List[Dict[str, Union[str, Callable]]]] = None) -> PyramidRouter:
        if isinstance(endpoints, dict):
            endpoints = [endpoints]
        elif isinstance(endpoints, Callable):
            endpoints = [{"path": "/", "method": "GET", "function": endpoints}]
        if not isinstance(endpoints, list) or not endpoints:
            endpoints = [{"path": "/", "method": "GET", "function": lambda request: {"status": "OK"}}]
        with PyramidConfigurator() as config:
            nendpoints = 0
            for endpoint in endpoints:
                if (endpoint_path := endpoint.get("path")) and (endpoint_function := endpoint.get("function")):
                    endpoint_method = endpoint.get("method", "GET")
                    def endpoint_wrapper(request):  # noqa
                        response = endpoint_function(request)
                        return PyramidResponse(json.dumps(response),
                                               content_type=f"{Portal.MIME_TYPE_JSON}; charset=utf-8")
                    endpoint_id = str(uuid())
                    config.add_route(endpoint_id, endpoint_path)
                    config.add_view(endpoint_wrapper, route_name=endpoint_id, request_method=endpoint_method)
                    nendpoints += 1
            if nendpoints == 0:
                return Portal._create_router_for_testing([])
            return config.make_wsgi_app()

    def start_for_testing(self, port: int = 7070, asynchronous: bool = False) -> Optional[Thread]:
        if isinstance(self.vapp, TestApp) and hasattr(self.vapp, "app") and isinstance(self.vapp.app, PyramidRouter):
            def start_server() -> None:  # noqa
                with wsgi_make_server("0.0.0.0", port or 7070, self.vapp.app) as server:
                    server.serve_forever()
            if asynchronous:
                server_thread = Thread(target=start_server)
                server_thread.daemon = True
                server_thread.start()
                return server_thread
            start_server()
