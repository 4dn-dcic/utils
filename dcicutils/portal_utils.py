from collections import deque
from pyramid.paster import get_app
from pyramid.router import Router
import re
import requests
from requests.models import Response as RequestResponse
from typing import Optional, Type, Union
from webtest.app import TestApp, TestResponse
from dcicutils.common import OrchestratedApp, APP_CGAP, APP_FOURFRONT, APP_SMAHT, ORCHESTRATED_APPS
from dcicutils.creds_utils import CGAPKeyManager, FourfrontKeyManager, SMaHTKeyManager
from dcicutils.ff_utils import get_metadata, get_schema, patch_metadata, post_metadata
from dcicutils.misc_utils import to_camel_case, VirtualApp
from dcicutils.zip_utils import temporary_file

Portal = Type["Portal"]  # Forward type reference for type hints.
FILE_SCHEMA_NAME = "File"


class Portal:
    """
    This is meant to be an uber wrapper for Portal access. It can be created in a variety of ways:
    1. From a (Portal) .ini file (e.g. development.ini)
    2. From a key dictionary, containing "key" and "secret" property values.
    3. From a key tuple, containing (in order) a key and secret values.
    4. From a keys file assumed to reside in ~/.{app}-keys.json where the given "app" value is either "smaht", "cgap",
       or "fourfront"; and where this file is assumed to contain a dictionary with a key equal to the given "env"
       value (e.g. smaht-localhost) and with a dictionary value containing "key" and "secret" property values; if
       an "app" value is not specified but the given "env" value begins with one of the app values then that value
       will be used, i.e. e.g. if env is "smaht-localhost" and app is unspecified than it is assumed to be "smaht".
    5. From a keys file as described above (#4) but rather than be identified by the given "env" value it
       is looked up by the given "server" name and the "server" key dictionary value in the key file.
    6. From a given "vapp" value (which is assumed to be a TestApp or VirtualApp).
    7. From another Portal object.
    8. From a a pyramid Router object.
    """
    def __init__(self,
                 arg: Optional[Union[VirtualApp, TestApp, Router, Portal, dict, tuple, str]] = None,
                 env: Optional[str] = None, app: Optional[OrchestratedApp] = None, server: Optional[str] = None,
                 key: Optional[Union[dict, tuple]] = None,
                 vapp: Optional[Union[VirtualApp, TestApp, Router, Portal, str]] = None,
                 portal: Optional[Union[VirtualApp, TestApp, Router, Portal, str]] = None) -> Portal:
        if vapp and not portal:
            portal = vapp
        if ((isinstance(arg, (VirtualApp, TestApp, Router, Portal)) or
             isinstance(arg, str) and arg.endswith(".ini")) and not portal):
            portal = arg
        elif isinstance(arg, str) and not env:
            env = arg
        elif (isinstance(arg, dict) or isinstance(arg, tuple)) and not key:
            key = arg
        if not app and env:
            if env.startswith(APP_SMAHT):
                app = APP_SMAHT
            elif env.startswith(APP_CGAP):
                app = APP_CGAP
            elif env.startswith(APP_FOURFRONT):
                app = APP_FOURFRONT
        if isinstance(portal, Portal):
            self._vapp = portal._vapp
            self._env = portal._env
            self._app = portal._app
            self._server = portal._server
            self._key = portal._key
            self._key_pair = portal._key_pair
            self._key_file = portal._key_file
            return
        self._vapp = None
        self._env = env
        self._app = app
        self._server = server
        self._key = None
        self._key_pair = None
        self._key_file = None
        if isinstance(portal, (VirtualApp, TestApp)):
            self._vapp = portal
        elif isinstance(portal, (Router, str)):
            self._vapp = Portal._create_vapp(portal)
        elif isinstance(key, dict):
            self._key = key
            self._key_pair = (key.get("key"), key.get("secret")) if key else None
            if key_server := key.get("server"):
                self._server = key_server
        elif isinstance(key, tuple) and len(key) >= 2:
            self._key = {"key": key[0], "secret": key[1]}
            self._key_pair = key
        elif isinstance(env, str):
            key_managers = {APP_CGAP: CGAPKeyManager, APP_FOURFRONT: FourfrontKeyManager, APP_SMAHT: SMaHTKeyManager}
            if not (key_manager := key_managers.get(self._app)) or not (key_manager := key_manager()):
                raise Exception(f"Invalid app name: {self._app} (valid: {', '.join(ORCHESTRATED_APPS)}).")
            if isinstance(env, str):
                self._key = key_manager.get_keydict_for_env(env)
                if key_server := self._key.get("server"):
                    self._server = key_server
            elif isinstance(self._server, str):
                self._key = key_manager.get_keydict_for_server(self._server)
            self._key_pair = key_manager.keydict_to_keypair(self._key) if self._key else None
            self._key_file = key_manager.keys_file

    @property
    def env(self):
        return self._env

    @property
    def app(self):
        return self._app

    @property
    def server(self):
        return self._server

    @property
    def key(self):
        return self._key

    @property
    def key_pair(self):
        return self._key_pair

    @property
    def key_file(self):
        return self._key_file

    @property
    def vapp(self):
        return self._vapp

    def get_metadata(self, object_id: str) -> Optional[dict]:
        return get_metadata(obj_id=object_id, vapp=self._vapp, key=self._key)

    def patch_metadata(self, object_id: str, data: str) -> Optional[dict]:
        if self._key:
            return patch_metadata(obj_id=object_id, patch_item=data, key=self._key)
        return self.patch(f"/{object_id}", data)

    def post_metadata(self, object_type: str, data: str) -> Optional[dict]:
        if self._key:
            return post_metadata(schema_name=object_type, post_item=data, key=self._key)
        return self.post(f"/{object_type}", data)

    def get(self, uri: str, follow: bool = True, **kwargs) -> Optional[Union[RequestResponse, TestResponse]]:
        if isinstance(self._vapp, (VirtualApp, TestApp)):
            response = self._vapp.get(self._uri(uri), **self._kwargs(**kwargs))
            if response and response.status_code in [301, 302, 303, 307, 308] and follow:
                response = response.follow()
            return self._response(response)
        return requests.get(self._uri(uri), allow_redirects=follow, **self._kwargs(**kwargs))

    def patch(self, uri: str, data: Optional[dict] = None,
              json: Optional[dict] = None, **kwargs) -> Optional[Union[RequestResponse, TestResponse]]:
        if isinstance(self._vapp, (VirtualApp, TestApp)):
            return self._vapp.patch_json(self._uri(uri), json or data, **self._kwargs(**kwargs))
        return requests.patch(self._uri(uri), json=json or data, **self._kwargs(**kwargs))

    def post(self, uri: str, data: Optional[dict] = None, json: Optional[dict] = None,
             files: Optional[dict] = None, **kwargs) -> Optional[Union[RequestResponse, TestResponse]]:
        if isinstance(self._vapp, (VirtualApp, TestApp)):
            if files:
                return self._vapp.post(self._uri(uri), json or data, upload_files=files, **self._kwargs(**kwargs))
            else:
                return self._vapp.post_json(self._uri(uri), json or data, upload_files=files, **self._kwargs(**kwargs))
        return requests.post(self._uri(uri), json=json or data, files=files, **self._kwargs(**kwargs))

    def get_schema(self, schema_name: str) -> Optional[dict]:
        return get_schema(self.schema_name(schema_name), portal_vapp=self._vapp, key=self._key)

    def get_schemas(self) -> dict:
        return self.get("/profiles/").json()

    @staticmethod
    def schema_name(name: str) -> str:
        return to_camel_case(name)

    def is_file_schema(self, schema_name: str) -> bool:
        if super_type_map := self.get_schemas_super_type_map():
            if file_super_type := super_type_map.get(FILE_SCHEMA_NAME):
                return self.schema_name(schema_name) in file_super_type
        return False

    def get_schemas_super_type_map(self) -> dict:
        """
        Returns the "super type map" for all of the known schemas (via /profiles).
        This is a dictionary of all types which have (one or more) sub-types whose value is
        an array of all of those sub-types (direct and all descendents), in breadth first order.
        """
        def breadth_first(super_type_map: dict, super_type_name: str) -> dict:
            result = []
            queue = deque(super_type_map.get(super_type_name, []))
            while queue:
                result.append(sub_type_name := queue.popleft())
                if sub_type_name in super_type_map:
                    queue.extend(super_type_map[sub_type_name])
            return result
        if not (schemas := self.get_schemas()):
            return {}
        super_type_map = {}
        for type_name in schemas:
            if super_type_name := schemas[type_name].get("rdfs:subClassOf"):
                super_type_name = super_type_name.replace("/profiles/", "").replace(".json", "")
                if super_type_name != "Item":
                    if not super_type_map.get(super_type_name):
                        super_type_map[super_type_name] = [type_name]
                    elif type_name not in super_type_map[super_type_name]:
                        super_type_map[super_type_name].append(type_name)
        super_type_map_flattened = {}
        for super_type_name in super_type_map:
            super_type_map_flattened[super_type_name] = breadth_first(super_type_map, super_type_name)
        return super_type_map_flattened

    def _uri(self, uri: str) -> str:
        if not isinstance(uri, str) or not uri:
            return "/"
        if uri.lower().startswith("http://") or uri.lower().startswith("https://"):
            return uri
        uri = re.sub(r"/+", "/", uri)
        return (self._server + ("/" if uri.startswith("/") else "") + uri) if self._server else uri

    def _kwargs(self, **kwargs) -> dict:
        result_kwargs = {"headers":
                         kwargs.get("headers", {"Content-type": "application/json", "Accept": "application/json"})}
        if self._key_pair:
            result_kwargs["auth"] = self._key_pair
        if isinstance(timeout := kwargs.get("timeout"), int):
            result_kwargs["timeout"] = timeout
        return result_kwargs

    def _response(self, response) -> Optional[RequestResponse]:
        if response and isinstance(getattr(response.__class__, "json"), property):
            class RequestResponseWrapper:  # For consistency change json property to method.
                def __init__(self, response, **kwargs):
                    super().__init__(**kwargs)
                    self._response = response
                def __getattr__(self, attr):  # noqa
                    return getattr(self._response, attr)
                def json(self):  # noqa
                    return self._response.json
            response = RequestResponseWrapper(response)
        return response

    @staticmethod
    def create_for_testing(ini_file: Optional[str] = None) -> Portal:
        if isinstance(ini_file, str):
            return Portal(Portal._create_vapp(ini_file))
        minimal_ini_for_unit_testing = "[app:app]\nuse = egg:encoded\nsqlalchemy.url = postgresql://dummy\n"
        with temporary_file(content=minimal_ini_for_unit_testing, suffix=".ini") as ini_file:
            return Portal(Portal._create_vapp(ini_file))

    @staticmethod
    def create_for_testing_local(ini_file: Optional[str] = None) -> Portal:
        if isinstance(ini_file, str) and ini_file:
            return Portal(Portal._create_vapp(ini_file))
        minimal_ini_for_testing_local = "\n".join([
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
        with temporary_file(content=minimal_ini_for_testing_local, suffix=".ini") as minimal_ini_file:
            return Portal(Portal._create_vapp(minimal_ini_file))

    @staticmethod
    def _create_vapp(value: Union[str, Router, TestApp] = "development.ini", app_name: str = "app") -> TestApp:
        if isinstance(value, TestApp):
            return value
        app = value if isinstance(value, Router) else get_app(value, app_name)
        return TestApp(app, {"HTTP_ACCEPT": "application/json", "REMOTE_USER": "TEST"})
