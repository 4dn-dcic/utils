# import contextlib
# import functools
import io
import json
import os

# from dcicutils.misc_utils import local_attrs
from dcicutils.exceptions import AppEnvKeyMissing, AppServerKeyMissing


# LOCAL_SERVER = "http://localhost:8000"
# LOCAL_PSEUDOENV = 'fourfront-cgaplocal'
#
# PRODUCTION_SERVER = 'https://cgap.hms.harvard.edu'
# PRODUCTION_ENV = 'fourfront-cgap'
#
# DEFAULT_ENV_VAR = 'SUBMITCGAP_ENV'
#
# keys_file_var='CGAP_KEYS_FILE'


class KeyManager:

    APP_NAME = None
    APP_TOKEN = None  # Set this to a string to override APP_NAME.lower() as the app token

    DEFAULT_ENV = None
    DEFAULT_ENV_VAR = None

    KEYS_FILE = None
    KEYS_FILE_VAR = None

    # Instance Methods

    def __init__(self, keys_file=None):
        self.keys_file = keys_file or os.environ.get(self.KEYS_FILE_VAR) or self.KEYS_FILE or self._default_keys_file()
        if not self.keys_file:
            raise ValueError("No KEYS_FILE attribute in {self}, and no {self.KEYS_FILE_VAR} environment variable.")

    # @contextlib.contextmanager
    # def alternate_keys_file_from_environ(self):
    #     filename = os.environ.get(self.KEYS_FILE_VAR) or None  # Treats empty string as undefined
    #     with self.alternate_keys_file(filename=filename):
    #         yield

    # Class Methods

    @classmethod
    def _default_keys_file(cls):
        app_token = cls.APP_TOKEN or cls.APP_NAME.lower()
        return os.path.expanduser(cls.KEYS_FILE or f"~/.{app_token}-keys.json")

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


class CGAPKeyManager(KeyManager):
    APP_NAME = 'CGAP'
    APP_TOKEN = 'cgap'
    DEFAULT_ENV_VAR = 'CGAP_DEFAULT_ENV'
    KEYS_FILE_VAR = 'CGAP_KEYS_FILE'


class FourfrontKeyManager(KeyManager):
    APP_NAME = 'Fourfront'
    APP_TOKEN = 'ff'
    DEFAULT_ENV_VAR = 'FF_DEFAULT_ENV'
    KEYS_FILE_VAR = 'FF_KEYS_FILE'
