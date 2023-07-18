import contextlib
import io
import json
import os

from dcicutils.exceptions import AppEnvKeyMissing, AppServerKeyMissing
from dcicutils.misc_utils import remove_suffix


_KEY_MANAGERS = {}


class KeyManager:

    APP_NAME = None
    APP_TOKEN = None  # Set this to a string to override APP_NAME.lower() as the app token

    KEYS_FILE = None
    KEYS_FILE_VAR = None

    _REGISTERED = False

    # Instance Methods

    def __init__(self, keys_file=None):
        """
        Defines what is done upon creation of a key manager instance.

        NOTE: The only such classes that are instantiable are those that have been registered.
              The reason is that the register decorator performs important initializations of class variables.

        Args:
            keys_file: the name of a filename to use instead of the name chosen by default
        """

        if not self._REGISTERED:
            raise RuntimeError("Only registered KeyManagers can be instantiated.")
        self.keys_file = keys_file or os.environ.get(self.KEYS_FILE_VAR) or self.KEYS_FILE
        if not self.keys_file:
            raise ValueError(f"No KEYS_FILE attribute in {self}, and no {self.KEYS_FILE_VAR} environment variable.")

    def get_keydicts(self) -> dict:
        """
        Parses and returns the keys file (held by self.keys_file)

        NOTE: No caching is done on a theory that you don't do this super-often, and you might have edited the
              keys file since last look, so you'd want most-up-to-date credentials. If those assumptions don't
              hold, please request additional support for this class rather than creating ad hoc solutions outside.

        Returns:
            a dictionary that maps an environment name to its auth information,
            which is a dict with keys 'key', 'secret', and 'server'.

        Raises:
            ValueError: if the file is ill-formatted (does not contain JSON or JSON is not a dictionary)
        """
        if not os.path.exists(self.keys_file):
            return {}
        with io.open(self.keys_file) as fp:
            keys = json.load(fp)
            if not isinstance(keys, dict):
                raise ValueError(f"The file {self.keys_file} did not contain a Python dictionary (in JSON format).")
            return keys

    def get_keydict_for_env(self, env):
        """
        Gets the appropriate auth info (as a dict) for talking to a given beanstalk environment.

        Args:
            env: the name of a beanstalk environment

        Returns:
            Auth information as a dict with keys 'key', 'secret', and 'server'.

        Raises:
            ValueError: if the file is ill-formatted (does not contain JSON or JSON is not a dictionary)
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

    # Class Methods

    @classmethod
    @contextlib.contextmanager
    def default_keys_file_for_testing(cls, filename):
        """
        Sets the default keys file for in cls.KEYS_FILE to the indicated filename.
        Ordinarily, in a non-testing environment, one would set an environment variable to do this,
        but in the testing environment that has been set already and to a value that has nothing to
        do with testing. So this bypasses the normal environment variable setup and sets it directly
        in the class and only for the duration of an evaluation context.
        """

        old_filename = cls.KEYS_FILE
        try:
            cls.KEYS_FILE = filename or cls._default_keys_file(for_testing=True)
            yield
        finally:
            cls.KEYS_FILE = old_filename

    @classmethod
    def register(cls, *, name):
        """
        A decorator that used to register a key manager class, so that it can be later looked up by the given name.
        This also does important class variable initialization for the key manager class,
        so no key manager class can be instantiated unless it has been registered in this way.

        Args:

            name:  a name token (a str) that should match the first part of the class name, before 'KeyManager'.
                   e.g., for CGAPKeyManager, the name should be 'cgap'.

        """
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
        # print(f"cls={cls!r}, name={class_name!r}")
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
        if not cls.KEYS_FILE:
            cls.KEYS_FILE = cls._default_keys_file()
        # print(f"Defining {class_name} "
        #       f"with APP_NAME={app_name!r} APP_TOKEN={app_token!r} KEYS_FILE_VAR={cls.KEYS_FILE_VAR!r}.")

    @classmethod
    def create(cls, name, **kwargs):
        """
        Allows creation of a registered key manager class using an alternate protocol that uses name lookup.
        For example, the following two are equivalent ways of creating an instance:
            CGAPKeyManager(**kwargs)
            KeyManager.create('cgap', **kwargs)
        """

        key_manager_class = _KEY_MANAGERS.get(name)
        if not key_manager_class:
            raise ValueError(f"There is no registered KeyManager class named {name!r}.")
        return key_manager_class(**kwargs)

    @classmethod
    def _default_keys_file(cls, for_testing=False):
        suffix = "-for-testing" if for_testing else ""
        return os.path.expanduser(f"~/.{cls.APP_TOKEN.lower()}-keys{suffix}.json")

    @classmethod
    def keypair_to_keydict(cls, auth_tuple, *, server):
        """
        Translates an auth tuple (key, secret) to a keydict with {"key": ..., "secret": ..., "server": ...}.
        Since the tuple must contain a secret and the keydict requires it, a server argument must be provided.
        That argument is required to be passed as a keyword.

        >>> KeyManager.keypair_to_keydict(('my-key', 'abra-cadabra'), server='http://whatever')
        {"key": "my-key", "secret": "abra-cadabra", "server": "http://whatever"}

        Args:
            auth_tuple (tuple): an auth tuple of the form (key, secret)
            server (str): a server name, such as "https://cgap.hms.harvard.edu" or "http://localhost:8000"
        Returns:
            a keydict of the form {"key": ..., "secret": ..., "server": ...}
        """

        auth_dict = {
            'key': auth_tuple[0],
            'secret': auth_tuple[1],
            'server': server,
        }
        return auth_dict

    @classmethod
    def keydict_to_keypair(cls, auth_dict):
        """
        Translates a keydict with {"key": ..., "secret": ..., "server": ...} to an auth tuple (key, secret).
        Since the tuple does not have a place for the server, it will be discarded.

        >>> KeyManager.keydict_to_keypair({"key": "my-key", "secret": "abra-cadabra", "server": "http://whatever"})
        ('my-key', 'abra-cadabra')

        Args:
            auth_dict (dict): a dictionary with entries for 'key' and 'secret'; any 'server' will be ignored
        Returns:
            a keypair of the form (key, secret)

        """
        return (
            auth_dict['key'],
            auth_dict['secret']
        )

    @classmethod
    def _check_env(cls, env):
        if not env or not isinstance(env, str):
            raise ValueError(f"Not a valid environment: {env}")

    @classmethod
    def _check_server(cls, server):
        if not server or not isinstance(server, str):
            raise ValueError(f"Not a valid server: {server}")


@KeyManager.register(name='cgap')
class CGAPKeyManager(KeyManager):
    pass


@KeyManager.register(name='fourfront')
class FourfrontKeyManager(KeyManager):
    pass


@KeyManager.register(name='smaht')
class SMaHTKeyManager(KeyManager):
    pass
