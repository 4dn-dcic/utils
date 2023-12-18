from collections import deque
import io
import json
from pyramid.paster import get_app
from pyramid.router import Router
import os
import re
import requests
from requests.models import Response as RequestResponse
from typing import Optional, Type, Union
from webtest.app import TestApp, TestResponse
from dcicutils.common import OrchestratedApp, ORCHESTRATED_APPS
from dcicutils.ff_utils import get_metadata, get_schema, patch_metadata, post_metadata
from dcicutils.misc_utils import to_camel_case, VirtualApp
from dcicutils.zip_utils import temporary_file

Portal = Type["Portal"]  # Forward type reference for type hints.


class Portal:
    """
    This is meant to be an uber wrapper for Portal access. It can be created in a variety of ways:
    1. From a (Portal) .ini file (e.g. development.ini)
    2. From a key dictionary, containing "key" and "secret" property values.
    3. From a key tuple, containing (in order) a key and secret values.
    4. From a keys file assumed to reside in ~/.{app}-keys.json where the given "app" value is either "smaht", "cgap",
       or "fourfront"; where is assumed to contain a dictionary with a key for the given "env" value, e.g. smaht-local;
       and with a dictionary value containing "key" and "secret" property values, and an optional "server" property;
       if an "app" value is not specified but the given "env" value begins with one of the app values then that value
       will be used, i.e. e.g. if "env" is "smaht-local" and app is unspecified than it is assumed to be "smaht".
    5. From a keys file as described above (#4) but rather than be identified by the given "env" value it
       is looked up via the given "server" name and the "server" key dictionary value in the key file.
    6. From a given "vapp" value (which is assumed to be a TestApp or VirtualApp).
    7. From another Portal object; or from a a pyramid Router object.
    """
    FILE_SCHEMA_NAME = "File"
    KEYS_FILE_DIRECTORY = os.path.expanduser(f"~")

    def __init__(self,
                 arg: Optional[Union[Portal, TestApp, VirtualApp, Router, dict, tuple, str]] = None,
                 env: Optional[str] = None, server: Optional[str] = None,
                 app: Optional[OrchestratedApp] = None) -> None:

        def init(unspecified: Optional[list] = []) -> None:
            self._ini_file = None
            self._key = None
            self._key_pair = None
            self._key_id = None
            self._secret = None
            self._keys_file = None
            self._env = None
            self._server = None
            self._app = None
            self._vapp = None
            for arg in unspecified:
                if arg is not None:
                    raise Exception("Portal init error; extraneous args.")

        def init_from_portal(portal: Portal, unspecified: Optional[list] = None) -> None:
            init(unspecified)
            self._ini_file = portal._ini_file
            self._key = portal._key
            self._key_pair = portal._key_pair
            self._key_id = portal._key_id
            self._secret = portal._secret
            self._keys_file = portal._keys_file
            self._env = portal._env
            self._server = portal._server
            self._app = portal._app
            self._vapp = portal._vapp

        def init_from_vapp(vapp: Union[TestApp, VirtualApp, Router], unspecified: Optional[list] = []) -> None:
            init(unspecified)
            self._vapp = Portal._create_testapp(vapp)

        def init_from_ini_file(ini_file: str, unspecified: Optional[list] = []) -> None:
            init(unspecified)
            self._ini_file = ini_file
            self._vapp = Portal._create_testapp(ini_file)

        def init_from_key(key: dict, server: Optional[str], unspecified: Optional[list] = []) -> None:
            init(unspecified)
            if (isinstance(key_id := key.get("key"), str) and key_id and
                isinstance(secret := key.get("secret"), str) and secret):  # noqa
                self._key = {"key": key_id, "secret": secret}
                self._key_id = key_id
                self._secret = secret
                self._key_pair = (key_id, secret)
                if ((isinstance(server, str) and server) or (isinstance(server := key.get("server"), str) and server)):
                    if server := normalize_server(server):
                        self._key["server"] = self._server = server
            if not self._key:
                raise Exception("Portal init error; from key.")

        def init_from_key_pair(key_pair: tuple, server: Optional[str], unspecified: Optional[list] = []) -> None:
            if len(key_pair) == 2:
                init_from_key({"key": key_pair[0], "secret": key_pair[1]}, server, unspecified)
            else:
                raise Exception("Portal init error; from key-pair.")

        def init_from_keys_file(keys_file: str, env: Optional[str], server: Optional[str],
                                unspecified: Optional[list] = []) -> None:
            try:
                with io.open(keys_file) as f:
                    keys = json.load(f)
            except Exception:
                raise Exception(f"Portal init error; cannot open keys-file: {keys_file}")
            if isinstance(env, str) and env and isinstance(key := keys.get(env), dict):
                init_from_key(key, server)
                self._keys_file = keys_file
                self._env = env
            elif isinstance(server, str) and server and (key := [k for k in keys if keys[k].get("server") == server]):
                init_from_key(key, server)
                self._keys_file = keys_file
            elif len(keys) == 1 and (env := next(iter(keys))) and isinstance(key := keys[env], dict) and key:
                init_from_key(key, server)
                self._keys_file = keys_file
                self._env = env
            else:
                raise Exception(f"Portal init error; {env or server or None} not found in keys-file: {keys_file}")

        def init_from_env_server_app(env: str, server: str, app: Optional[str],
                                     unspecified: Optional[list] = None) -> None:
            return init_from_keys_file(self._default_keys_file(app, env), env, server, unspecified)

        def normalize_server(server: str) -> Optional[str]:
            prefix = ""
            if (lserver := server.lower()).startswith("http://"):
                prefix = "http://"
            elif lserver.startswith("https://"):
                prefix = "https://"
            if prefix:
                if (server := re.sub(r"/+", "/", server[len(prefix):])).startswith("/"):
                    server = server[1:]
                if len(server) > 1 and server.endswith("/"):
                    server = server[:-1]
                return prefix + server if server else None

        if isinstance(arg, Portal):
            init_from_portal(arg, unspecified=[env, server, app])
        elif isinstance(arg, (TestApp, VirtualApp, Router)):
            init_from_vapp(arg, unspecified=[env, server, app])
        elif isinstance(arg, str) and arg.endswith(".ini"):
            init_from_ini_file(arg, unspecified=[env, server, app])
        elif isinstance(arg, dict):
            init_from_key(arg, server, unspecified=[env, app])
        elif isinstance(arg, tuple):
            init_from_key_pair(arg, server, unspecified=[env, app])
        elif isinstance(arg, str) and arg.endswith(".json"):
            init_from_keys_file(arg, env, server, unspecified=[app])
        elif isinstance(arg, str) and arg:
            init_from_env_server_app(arg, server, app, unspecified=[env])
        elif isinstance(env, str) and env:
            init_from_env_server_app(env, server, app, unspecified=[arg])
        else:
            raise Exception("Portal init error; invalid args.")

    @property
    def ini_file(self) -> Optional[str]:
        return self._ini_file

    @property
    def key(self) -> Optional[dict]:
        return self._key

    @property
    def key_pair(self) -> Optional[tuple]:
        return self._key_pair

    @property
    def key_id(self) -> Optional[str]:
        return self._key_id

    @property
    def secret(self) -> Optional[str]:
        return self._secret

    @property
    def keys_file(self) -> Optional[str]:
        return self._keys_file

    @property
    def env(self) -> Optional[str]:
        return self._env

    @property
    def server(self) -> Optional[str]:
        return self._server

    @property
    def app(self) -> Optional[str]:
        return self._app

    @property
    def vapp(self) -> Optional[TestApp]:
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
        if self._vapp:
            response = self._vapp.get(self.url(uri), **self._kwargs(**kwargs))
            if response and response.status_code in [301, 302, 303, 307, 308] and follow:
                response = response.follow()
            return self._response(response)
        return requests.get(self.url(uri), allow_redirects=follow, **self._kwargs(**kwargs))

    def patch(self, uri: str, data: Optional[dict] = None,
              json: Optional[dict] = None, **kwargs) -> Optional[Union[RequestResponse, TestResponse]]:
        if self._vapp:
            return self._vapp.patch_json(self.url(uri), json or data, **self._kwargs(**kwargs))
        return requests.patch(self.url(uri), data=data, json=json, **self._kwargs(**kwargs))

    def post(self, uri: str, data: Optional[dict] = None, json: Optional[dict] = None,
             files: Optional[dict] = None, **kwargs) -> Optional[Union[RequestResponse, TestResponse]]:
        if self._vapp:
            if files:
                return self._vapp.post(self.url(uri), json or data, upload_files=files, **self._kwargs(**kwargs))
            else:
                return self._vapp.post_json(self.url(uri), json or data, upload_files=files, **self._kwargs(**kwargs))
        return requests.post(self.url(uri), data=data, json=json, files=files, **self._kwargs(**kwargs))

    def get_schema(self, schema_name: str) -> Optional[dict]:
        return get_schema(self.schema_name(schema_name), portal_vapp=self._vapp, key=self._key)

    def get_schemas(self) -> dict:
        return self.get("/profiles/").json()

    @staticmethod
    def schema_name(name: str) -> str:
        return to_camel_case(name)

    def is_file_schema(self, schema_name: str) -> bool:
        if super_type_map := self.get_schemas_super_type_map():
            if file_super_type := super_type_map.get(Portal.FILE_SCHEMA_NAME):
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

    def ping(self) -> bool:
        try:
            return self.get("/health").status_code == 200
        except Exception:
            return False

    def url(self, uri: str) -> str:
        if not isinstance(uri, str) or not uri:
            return "/"
        if (luri := uri.lower()).startswith("http://") or luri.startswith("https://"):
            return uri
        if not (uri := re.sub(r"/+", "/", uri)).startswith("/"):
            uri = "/"
        return self._server + uri if self._server else uri

    def _kwargs(self, **kwargs) -> dict:
        result_kwargs = {"headers":
                         kwargs.get("headers", {"Content-type": "application/json", "Accept": "application/json"})}
        if self._key_pair:
            result_kwargs["auth"] = self._key_pair
        if isinstance(timeout := kwargs.get("timeout"), int):
            result_kwargs["timeout"] = timeout
        return result_kwargs

    def _default_keys_file(self, app: Optional[str], env: Optional[str] = None) -> Optional[str]:
        def is_valid_app(app: Optional[str]) -> bool:  # noqa
            return app and app.lower() in [name.lower() for name in ORCHESTRATED_APPS]
        def infer_app_from_env(env: str) -> Optional[str]:  # noqa
            if isinstance(env, str) and (lenv := env.lower()):
                if app := [app for app in ORCHESTRATED_APPS if lenv.startswith(app.lower())]:
                    return app[0]
        if is_valid_app(app) or (app := infer_app_from_env(env)):
            return os.path.join(Portal.KEYS_FILE_DIRECTORY, f".{app.lower()}-keys.json")

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
            return Portal(Portal._create_testapp(ini_file))
        minimal_ini_for_unit_testing = "[app:app]\nuse = egg:encoded\nsqlalchemy.url = postgresql://dummy\n"
        with temporary_file(content=minimal_ini_for_unit_testing, suffix=".ini") as ini_file:
            return Portal(Portal._create_testapp(ini_file))

    @staticmethod
    def create_for_testing_local(ini_file: Optional[str] = None) -> Portal:
        if isinstance(ini_file, str) and ini_file:
            return Portal(Portal._create_testapp(ini_file))
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
            return Portal(Portal._create_testapp(minimal_ini_file))

    @staticmethod
    def _create_testapp(arg: Union[TestApp, VirtualApp, Router, str] = None, app_name: Optional[str] = None) -> TestApp:
        if isinstance(arg, TestApp):
            return arg
        elif isinstance(arg, VirtualApp):
            if not isinstance(arg.wrapped_app, TestApp):
                raise Exception("Portal._create_testapp VirtualApp argument error.")
            return arg.wrapped_app
        if isinstance(arg, Router):
            router = arg
        elif isinstance(arg, str) or arg is None:
            router = get_app(arg or "development.ini", app_name or "app")
        else:
            raise Exception("Portal._create_testapp argument error.")
        return TestApp(router, {"HTTP_ACCEPT": "application/json", "REMOTE_USER": "TEST"})
