# import contextlib
# import functools
import io
import json
import os

# from dcicutils.misc_utils import local_attrs
from dcicutils.exceptions import AppEnvKeyMissing, AppServerKeyMissing
from dcicutils.misc_utils import remove_suffix


# LOCAL_SERVER = "http://localhost:8000"
# LOCAL_PSEUDOENV = 'fourfront-cgaplocal'
#
# PRODUCTION_SERVER = 'https://cgap.hms.harvard.edu'
# PRODUCTION_ENV = 'fourfront-cgap'
#
# keys_file_var='CGAP_KEYS_FILE'


_KEY_MANAGERS = {}


class KeyManager:

    APP_NAME = None
    APP_TOKEN = None  # Set this to a string to override APP_NAME.lower() as the app token

    KEYS_FILE_VAR = None

    _REGISTERED = False

    # Instance Methods

    def __init__(self, keys_file=None):
        if not self._REGISTERED:
            raise RuntimeError("Only registered KeyManagers can be instantiated.")
        self.keys_file = keys_file or os.environ.get(self.KEYS_FILE_VAR) or self.KEYS_FILE
        if not self.keys_file:
            raise ValueError(f"No KEYS_FILE attribute in {self}, and no {self.KEYS_FILE_VAR} environment variable.")

    @property
    def KEYS_FILE(self):  # noQA
        # By default this will be computed dynamically, but a KeyManager class can declare a static value by doing
        # something like this:
        #
        #     class MyKeyManager(KeyManager):
        #         KEY_FILE = 'some file'
        #
        return self._default_keys_file()

    @classmethod
    def register(cls, *, name):
        def _register_class(key_manager_class):
            assert issubclass(key_manager_class, KeyManager)
            if name in _KEY_MANAGERS:
                raise ValueError(f"A KeyManager named {name!r} has already been defined.")
            key_manager_class._init_class_variables()
            key_manager_class._REGISTERED = True
            _KEY_MANAGERS[name] = cls
            return key_manager_class
        return _register_class

    @classmethod
    def _init_class_variables(cls):
        class_name = cls.__name__
        print(f"cls={cls!r}, name={class_name!r}")
        suffix = "KeyManager"
        if not class_name.endswith(suffix):
            raise ValueError(f"The name, {class_name!r}, of a KeyManager class to be registered"
                             f" does not end in {suffix!r}.")
        app_name = remove_suffix(suffix=suffix, text=class_name, required=True)
        if cls.APP_NAME is None:
            cls.APP_NAME = app_name
        elif cls.APP_NAME.lower() != app_name.lower():
            raise ValueError(f"A KeyManager class with APP_NAME = {cls.APP_NAME!r} expects a name"
                             f" like {cls.APP_NAME}{suffix}.")
        else:
            app_name = cls.APP_NAME
        if cls.APP_TOKEN is None:
            cls.APP_TOKEN = app_token = app_name.upper()
        else:
            app_token = cls.APP_TOKEN
            if not app_token.isupper():
                raise ValueError(f"The APP_TOKEN {app_token!r} must be all-uppercase.")
            elif app_token[:1] != app_name[0].upper():
                raise ValueError(f"The APP_TOKEN {app_token!r} must have the same first letter as {app_name!r}")
        app_prefix = app_token + "_"
        if not cls.KEYS_FILE_VAR:
            cls.KEYS_FILE_VAR = f"{app_prefix}KEYS_FILE"
        elif not cls.KEYS_FILE_VAR.startswith(app_prefix):
            raise ValueError("The {class_name}.KEYS_FILE_VAR must begin with {app_prefix!r}.")
        # print(f"Defining {class_name} "
        #       f"with APP_NAME={app_name!r} APP_TOKEN={app_token!r} KEYS_FILE_VAR={cls.KEYS_FILE_VAR!r}.")

    @classmethod
    def create(cls, name, **kwargs):
        key_manager_class = _KEY_MANAGERS.get(name)
        if not key_manager_class:
            raise ValueError(f"There is no registered KeyManager class named {name!r}.")
        return key_manager_class(**kwargs)

    # @contextlib.contextmanager
    # def alternate_keys_file_from_environ(self):
    #     filename = os.environ.get(self.KEYS_FILE_VAR) or None  # Treats empty string as undefined
    #     with self.alternate_keys_file(filename=filename):
    #         yield

    # Class Methods

    @classmethod
    def _default_keys_file(cls):
        return os.path.expanduser(f"~/.{cls.APP_TOKEN.lower()}-keys.json")

    # Replaced by an instance variable. Rewrites weill be needed.
    #
    # @classmethod
    # def keys_file(cls):
    #     app_token = cls.APP_TOKEN or cls.APP_NAME.lower()
    #     return os.path.expanduser(cls.KEYS_FILE or f"~/.{app_token}-keys.json")

    # # Formerly a function / classmethod, now a normal instance method of an instance (of KeyManager)
    # @contextlib.contextmanager
    # def alternate_keys_file(self, filename):
    #     if filename is None:
    #         yield  # If no alternate filename given, change nothing
    #     else:
    #         with local_attrs(self, keys_file=filename):
    #             yield

    # @classmethod
    # def UsingKeysFile(cls, key_manager):
    #     def _UsingKeysFile(fn):  # TO DO: declare as decorator
    #         @functools.wraps(fn)
    #         def wrapped(*args, **kwargs):
    #             with key_manager.alternate_keys_file_from_environ():
    #                 return fn(*args, **kwargs)
    #         return wrapped
    #     return _UsingKeysFile

    @classmethod  # tested. good as class method
    def keypair_to_keydict(cls, auth_tuple, *, server):
        auth_dict = {
            'key': auth_tuple[0],
            'secret': auth_tuple[1],
            'server': server,
        }
        return auth_dict

    @classmethod  # tested. good as class method
    def keydict_to_keypair(cls, auth_dict):
        return (
            auth_dict['key'],
            auth_dict['secret']
        )

    # formerly a class method. no longer can be.
    def get_keydicts(self):  # tested
        if not os.path.exists(self.keys_file):
            return {}
        with io.open(self.keys_file) as fp:
            keys = json.load(fp)
            return keys

    @classmethod
    def _check_env(cls, env):
        if not env or not isinstance(env, str):
            raise ValueError(f"Not a valid environment: {env}")

    def get_keydict_for_env(self, env):
        """
        Gets the appropriate auth info (as a dict) for talking to a given beanstalk environment.

        Args:
            env: the name of a beanstalk environment

        Returns:
            Auth information as a dict with keys 'key', 'secret', and 'server'.
        """

        self._check_env(env)

        keydicts = self.get_keydicts()
        keydict = keydicts.get(env)
        if not keydict:
            raise AppEnvKeyMissing(env=env, key_manager=self)
        return keydict

    def get_keypair_for_env(self, env):
        """
        Gets the appropriate auth info (as a pair/tuple) for talking to a given beanstalk environment.

        Args:
            env: the name of an environment

        Returns:
            Auth information as a (key, secret) tuple.
        """

        return self.keydict_to_keypair(self.get_keydict_for_env(env=env))

    @classmethod
    def _check_server(cls, server):
        if not server or not isinstance(server, str):
            raise ValueError(f"Not a valid server: {server}")

    def get_keydict_for_server(self, server):  # tested
        """
        Gets the appropriate auth info (as a dict) for talking to a given beanstalk environment.

        Args:
            server: the name of a server

        Returns:
            Auth information.
            The auth is a keypair, though we might change this to include a JWT token in the the future.
        """

        self._check_server(server)

        keydicts = self.get_keydicts()
        server_to_find = server.rstrip('/')
        for keydict in keydicts.values():
            if keydict['server'].rstrip('/') == server_to_find:
                return keydict
        raise AppServerKeyMissing(server=server, key_manager=self)

    def get_keypair_for_server(self, server):  # tested
        """
        Gets the appropriate auth info (as a pair/tuple) for talking to a given beanstalk environment.

        Args:
            server: the name of a server

        Returns:
            Auth information as a (key, secret) tuple.
        """

        return self.keydict_to_keypair(self.get_keydict_for_server(server=server))


@KeyManager.register(name='cgap')
class CGAPKeyManager(KeyManager):
    pass


@KeyManager.register(name='fourfront')
class FourfrontKeyManager(KeyManager):
    APP_TOKEN = 'FF'
