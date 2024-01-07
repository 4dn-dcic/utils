from collections import deque
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
from dcicutils.common import OrchestratedApp, ORCHESTRATED_APPS
from dcicutils.ff_utils import get_metadata, get_schema, patch_metadata, post_metadata
from dcicutils.misc_utils import to_camel_case, VirtualApp
from dcicutils.tmpfile_utils import temporary_file

Portal = Type["Portal"]  # Forward type reference for type hints.
OptionalResponse = Optional[Union[Response, TestResponse]]


class Portal:
    """
    This is meant to be an Über wrapper for Portal access. It can be created in a variety of ways:
    1. From a (Portal) .ini file (e.g. development.ini).
    2. From a key dictionary, containing "key" and "secret" property values.
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
    FILE_SCHEMA_NAME = "File"
    KEYS_FILE_DIRECTORY = "~"
    MIME_TYPE_JSON = "application/json"

    def __init__(self,
                 arg: Optional[Union[Portal, TestApp, VirtualApp, PyramidRouter, dict, tuple, str]] = None,
                 env: Optional[str] = None, server: Optional[str] = None,
                 app: Optional[OrchestratedApp] = None) -> None:

        def init(unspecified: Optional[list] = []) -> None:
            self._ini_file = None
            self._key = None
            self._keys_file = None
            self._env = None
            self._server = None
            self._app = None
            self._vapp = None
            for arg in unspecified:
                if arg is not None:
                    raise Exception("Portal init error; extraneous args.")

        def init_from_portal(portal: Portal, unspecified: Optional[list] = None) -> None:
            init(unspecified=unspecified)
            self._ini_file = portal._ini_file
            self._key = portal._key
            self._keys_file = portal._keys_file
            self._env = portal._env
            self._server = portal._server
            self._app = portal._app
            self._vapp = portal._vapp

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
                    if server := normalize_server(server):
                        if isinstance(key_server := key.get("server"), str) and key_server:
                            if normalize_server(key_server) != server:
                                raise Exception(f"Portal server inconsistency: {server} vs {key_server}")
                        self._key["server"] = self._server = server
            if not self._key:
                raise Exception("Portal init error; from key.")

        def init_from_key_pair(key_pair: tuple, server: Optional[str], unspecified: Optional[list] = []) -> None:
            if len(key_pair) == 2:
                init_from_key({"key": key_pair[0], "secret": key_pair[1]}, server, unspecified=unspecified)
            else:
                raise Exception("Portal init error; from key-pair.")

        def init_from_keys_file(keys_file: str, env: Optional[str], server: Optional[str],
                                unspecified: Optional[list] = []) -> None:
            try:
                with io.open(keys_file := os.path.expanduser(keys_file)) as f:
                    keys = json.load(f)
            except Exception:
                raise Exception(f"Portal init error; cannot open keys-file: {keys_file}")
            if isinstance(env, str) and env and isinstance(key := keys.get(env), dict):
                init_from_key(key, server)
                self._keys_file = keys_file
                self._env = env
            elif (isinstance(server, str) and (server := normalize_server(server)) and
                  (key := [keys[k] for k in keys if normalize_server(keys[k].get("server")) == server])):
                init_from_key(key[0], server)
                self._keys_file = keys_file
            elif len(keys) == 1 and (env := next(iter(keys))) and isinstance(key := keys[env], dict) and key:
                init_from_key(key, server)
                self._keys_file = keys_file
                self._env = env
            else:
                raise Exception(f"Portal init error; {env or server or None} not found in keys-file: {keys_file}")

        def init_from_env_server_app(env: str, server: str, app: Optional[str],
                                     unspecified: Optional[list] = None) -> None:
            if keys_file := self._default_keys_file(app, env):
                init_from_keys_file(keys_file, env, server, unspecified=unspecified)
            else:
                init(unspecified=unspecified)
                self._env = env
                self._server = server

        def normalize_server(server: str) -> Optional[str]:
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

        if app and not Portal._is_valid_app(app):
            raise Exception(f"Portal init error; invalid app: {app}")
        self._app = app
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
        else:
            raise Exception("Portal init error; invalid args.")

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

    def get(self, url: str, follow: bool = True, raise_for_status: bool = False, **kwargs) -> OptionalResponse:
        url = self.url(url)
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

    def get_metadata(self, object_id: str) -> Optional[dict]:
        return get_metadata(obj_id=object_id, vapp=self.vapp, key=self.key)

    def patch_metadata(self, object_id: str, data: str) -> Optional[dict]:
        if self.key:
            return patch_metadata(obj_id=object_id, patch_item=data, key=self.key)
        return self.patch(f"/{object_id}", data).json()

    def post_metadata(self, object_type: str, data: str) -> Optional[dict]:
        if self.key:
            return post_metadata(schema_name=object_type, post_item=data, key=self.key)
        return self.post(f"/{object_type}", data).json()

    def get_health(self) -> OptionalResponse:
        return self.get("/health")

    def ping(self) -> bool:
        try:
            return self.get_health().status_code == 200
        except Exception:
            return False

    def get_schema(self, schema_name: str) -> Optional[dict]:
        return get_schema(self.schema_name(schema_name), portal_vapp=self.vapp, key=self.key)

    def get_schemas(self) -> dict:
        return self.get("/profiles/").json()

    @staticmethod
    def schema_name(name: str) -> str:
        return to_camel_case(name.replace(" ", "") if not name.endswith(".json") else name[:-5])

    def is_schema_type(self, value: dict, schema_type: str) -> bool:
        """
        Returns True iff the given object isa type of the given schema type.
        """
        if isinstance(value, dict) and (value_types := value.get("@type")):
            if isinstance(value_types, str):
                return self.is_specified_schema(value_types, schema_type)
            elif isinstance(value_types, list):
                for value_type in value_types:
                    if self.is_specified_schema(value_type, schema_type):
                        return True
        return False

    def is_specified_schema(self, schema_name: str, schema_type: str) -> bool:
        """
        Returns True iff the given schema name isa type of the given schema type name,
        i.e. has an ancestor which is of type that given type.
        """
        schema_name = self.schema_name(schema_name)
        schema_type = self.schema_name(schema_type)
        if schema_name == schema_type:
            return True
        if super_type_map := self.get_schemas_super_type_map():
            if super_type := super_type_map.get(schema_type):
                return self.schema_name(schema_name) in super_type
        return False

    def is_file_schema(self, schema_name: str) -> bool:
        """
        Returns True iff the given schema name isa File type, i.e. has an ancestor which is of type File.
        """
        return self.is_specified_schema(schema_name, Portal.FILE_SCHEMA_NAME)

    def get_schemas_super_type_map(self) -> dict:
        """
        Returns the "super type map" for all of the known schemas (via /profiles).
        This is a dictionary with property names which are all known schema type names which
        have (one or more) sub-types, and the value each such property name is an array of
        all of those sub-types (direct and all descendents), in breadth first order.
        """
        def list_breadth_first(super_type_map: dict, super_type_name: str) -> dict:
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
            super_type_map_flattened[super_type_name] = list_breadth_first(super_type_map, super_type_name)
        return super_type_map_flattened

    def url(self, url: str) -> str:
        if not isinstance(url, str) or not url:
            return "/"
        if (lowercase_url := url.lower()).startswith("http://") or lowercase_url.startswith("https://"):
            return url
        if not (url := re.sub(r"/+", "/", url)).startswith("/"):
            url = "/"
        return self.server + url if self.server else url

    def _kwargs(self, **kwargs) -> dict:
        result_kwargs = {"headers": kwargs.get("headers", {"Content-type": Portal.MIME_TYPE_JSON,
                                                           "Accept": Portal.MIME_TYPE_JSON})}
        if self.key_pair:
            result_kwargs["auth"] = self.key_pair
        if isinstance(timeout := kwargs.get("timeout"), int):
            result_kwargs["timeout"] = timeout
        return result_kwargs

    def _default_keys_file(self, app: Optional[str], env: Optional[str] = None) -> Optional[str]:
        def infer_app_from_env(env: str) -> Optional[str]:  # noqa
            if isinstance(env, str) and (lowercase_env := env.lower()):
                if app := [app for app in ORCHESTRATED_APPS if lowercase_env.startswith(app.lower())]:
                    return app[0]
        if Portal._is_valid_app(app) or (app := infer_app_from_env(env)):
            default_keys_file = os.path.join(Portal.KEYS_FILE_DIRECTORY, f".{app.lower()}-keys.json")
            return default_keys_file if os.path.exists(default_keys_file) else None

    @staticmethod
    def _is_valid_app(app: Optional[str]) -> bool:
        return isinstance(app, str) and app.lower() in [name.lower() for name in ORCHESTRATED_APPS]

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
    def create_for_testing(arg: Optional[Union[str, bool, List[dict], dict, Callable]] = None) -> Portal:
        if isinstance(arg, list) or isinstance(arg, dict) or isinstance(arg, Callable):
            return Portal(Portal._create_router_for_testing(arg))
        elif isinstance(arg, str) and arg.endswith(".ini"):
            return Portal(Portal._create_vapp(arg))
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
            return Portal(Portal._create_vapp(ini_file))

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
