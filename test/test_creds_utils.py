import io
import json
import os
import pytest

from dcicutils.creds_utils import KeyManager, CGAPKeyManager, FourfrontKeyManager
from dcicutils.exceptions import AppEnvKeyMissing, AppServerKeyMissing  # , AppKeyMissing
from dcicutils.qa_utils import override_environ, MockFileSystem


SAMPLE_CGAP_DEFAULT_ENV = 'cgap-sample'
SAMPLE_CGAP_KEYS_FILE = 'cgap.keys'
SAMPLE_CGAP_PRODUCTION_ENV = 'cgap-production'
SAMPLE_CGAP_PRODUCTION_SERVER = 'https://cgap-prod.hms.harvard.edu'
SAMPLE_CGAP_LOCAL_SERVER = "http://localhost:8001"
SAMPLE_CGAP_LOCAL_PSEUDOENV = 'cgap-local'

SAMPLE_FOURFRONT_DEFAULT_ENV = 'fourfront-sample'
SAMPLE_FOURFRONT_KEYS_FILE = 'fourfront.keys'
SAMPLE_FOURFRONT_PRODUCTION_ENV = 'fourfront-production'
SAMPLE_FOURFRONT_PRODUCTION_SERVER = 'https://cgap-prod.hms.harvard.edu'
SAMPLE_FOURFRONT_LOCAL_SERVER = "http://localhost:8002"
SAMPLE_FOURFRONT_LOCAL_PSEUDOENV = 'fourfront-local'


def _make_sample_cgap_key_manager():
    return CGAPKeyManager(keys_file=SAMPLE_CGAP_KEYS_FILE)


def _make_sample_fourfront_key_manager():
    return FourfrontKeyManager(keys_file=SAMPLE_FOURFRONT_KEYS_FILE)


def test_cgap_keymanager_creation():

    sample_cgap_key_manager_1 = CGAPKeyManager()

    assert sample_cgap_key_manager_1.keys_file == CGAPKeyManager._default_keys_file()

    sample_cgap_key_manager_2 = _make_sample_cgap_key_manager()

    assert sample_cgap_key_manager_2.keys_file == SAMPLE_CGAP_KEYS_FILE

    with override_environ(CGAP_KEYS_FILE=SAMPLE_CGAP_KEYS_FILE):

        sample_cgap_key_manager_3 = CGAPKeyManager()

        assert sample_cgap_key_manager_3.keys_file == SAMPLE_CGAP_KEYS_FILE
        # Make sure class default is different than test value. More of a test-integrity test than an absolute need.
        assert sample_cgap_key_manager_3.keys_file != CGAPKeyManager._default_keys_file()

    other_keys_file = "other-cgap-keys.json"

    class MyCGAPKeyManager(CGAPKeyManager):
        KEYS_FILE = other_keys_file

    sample_cgap_key_manager_4 = MyCGAPKeyManager()  # Tests that no error is raised

    assert sample_cgap_key_manager_4.keys_file == other_keys_file


def test_fourfront_keymanager_creation():

    sample_ff_key_manager_1 = FourfrontKeyManager()

    assert sample_ff_key_manager_1.keys_file == FourfrontKeyManager._default_keys_file()

    sample_ff_key_manager_2 = _make_sample_fourfront_key_manager()

    assert sample_ff_key_manager_2.keys_file == SAMPLE_FOURFRONT_KEYS_FILE

    with override_environ(FF_DEFAULT_ENV=SAMPLE_FOURFRONT_LOCAL_PSEUDOENV):

        sample_ff_key_manager_3 = FourfrontKeyManager()

        assert sample_ff_key_manager_3.keys_file == FourfrontKeyManager._default_keys_file()
        # Make sure class default is different than test value. More of a test-integrity test than an absolute need.
        assert sample_ff_key_manager_3.keys_file != SAMPLE_FOURFRONT_KEYS_FILE

    other_keys_file = "other-ff-keys.json"

    class MyFourfrontKeyManager(FourfrontKeyManager):
        KEYS_FILE = other_keys_file

    sample_ff_key_manager_4 = MyFourfrontKeyManager()  # Tests that no error is raised

    assert sample_ff_key_manager_4.keys_file == other_keys_file


def test_keymanager_keys_file():

    key_manager = _make_sample_cgap_key_manager()

    original_file = key_manager.keys_file

    assert isinstance(original_file, str)

    assert key_manager.keys_file == original_file

    with override_environ(CGAP_KEYS_FILE=None):
        assert os.environ.get('CGAP_KEYS_FILE') is None
        assert key_manager.keys_file == original_file

    assert key_manager.keys_file == original_file

    with override_environ(CGAP_KEYS_FILE=""):
        assert os.environ.get('CGAP_KEYS_FILE') == ""
        assert key_manager.keys_file == original_file

    assert key_manager.keys_file == original_file

    alternate_file = 'some-other-file'

    with override_environ(CGAP_KEYS_FILE=alternate_file):
        assert os.environ.get('CGAP_KEYS_FILE') == alternate_file

        key_manager = CGAPKeyManager()
        assert key_manager.keys_file == alternate_file

    key_manager = _make_sample_cgap_key_manager()

    assert key_manager.keys_file == original_file


def test_keydict_to_keypair():  # KeyManager.keydict_to_keypair is a class method.

    sample_key, sample_secret, sample_server = 'foo', 'bar', 'baz'
    sample_keydict = {'key': sample_key, 'secret': sample_secret, 'server': sample_server}
    sample_keypair = (sample_key, sample_secret)

    assert KeyManager.keydict_to_keypair(sample_keydict) == sample_keypair


def test_keypair_to_keydict():  # KeyManager.keypair_to_keydict is a class method.

    sample_key, sample_secret, sample_server = 'foo', 'bar', 'baz'
    sample_keydict = {'key': sample_key, 'secret': sample_secret, 'server': sample_server}
    sample_keypair = (sample_key, sample_secret)

    assert KeyManager.keypair_to_keydict(sample_keypair, server=sample_server) == sample_keydict


def test_get_cgap_keydicts_missing():

    mfs = MockFileSystem()

    with mfs.mock_exists_open_remove():

        key_manager = _make_sample_cgap_key_manager()

        # When keys file is missing, it appears empty.
        assert key_manager.get_keydicts() == {}

        # When keys file contains valid JSON, but not a dictionary, ValueError is raised.
        with io.open(key_manager.keys_file, 'w') as fp:
            fp.write('"i am not a dictionary"')
        with pytest.raises(ValueError):
            key_manager.get_keydicts()

        # When keys file contains invalid JSON, ValueError is raised.
        # Note: In fact, the error class is more specific, but it inherits from ValueError.
        with io.open(key_manager.keys_file, 'w') as fp:
            fp.write('i am not json')
        with pytest.raises(ValueError):
            key_manager.get_keydicts()

        with io.open(key_manager.keys_file, 'w') as fp:
            keys_file_content_for_testing = {
                "fourfront-cgapdev":
                    {"key": "my-key", "secret": "my-pw", "server": "http://localhost:8000"}
            }
            fp.write(json.dumps(keys_file_content_for_testing))
        parsed_keydicts = key_manager.get_keydicts()
        assert parsed_keydicts == keys_file_content_for_testing


SAMPLE_MISSING_ENV = 'fourfront-cgapwolf'
SAMPLE_MISSING_SERVER = "http://localhost:6666"

SAMPLE_CGAP_PAIR = ('key000', 'secret000')

SAMPLE_CGAP_DICT = {
    'key': SAMPLE_CGAP_PAIR[0],
    'secret': SAMPLE_CGAP_PAIR[1],
    'server': SAMPLE_CGAP_PRODUCTION_SERVER,
}

SAMPLE_CGAP_FOO_ENV = 'fourfront-cgapfoo'

SAMPLE_CGAP_FOO_PAIR = ('key123', 'secret123')

SAMPLE_CGAP_FOO_SERVER = 'https://foo.cgap.hms.harvard.edu'

SAMPLE_CGAP_FOO_DICT = {
    'key': SAMPLE_CGAP_FOO_PAIR[0],
    'secret': SAMPLE_CGAP_FOO_PAIR[1],
    'server': SAMPLE_CGAP_FOO_SERVER,
}

SAMPLE_CGAP_LOCAL_PAIR = ('key456', 'secret456')

# SAMPLE_CGAP_LOCAL_SERVER dfeined above
# cgap_local_server = 'http://localhost:8000'

SAMPLE_CGAP_LOCAL_DICT = {
    'key': SAMPLE_CGAP_LOCAL_PAIR[0],
    'secret': SAMPLE_CGAP_LOCAL_PAIR[1],
    'server': SAMPLE_CGAP_LOCAL_SERVER,
}

SAMPLE_CGAP_KEYS_FILE_CONTENT = {
    SAMPLE_CGAP_FOO_ENV: SAMPLE_CGAP_FOO_DICT,
    SAMPLE_CGAP_LOCAL_PSEUDOENV: SAMPLE_CGAP_LOCAL_DICT,
    SAMPLE_CGAP_PRODUCTION_ENV: SAMPLE_CGAP_DICT,
}

SAMPLE_CGAP_KEYS_FILE_TEXT = json.dumps(SAMPLE_CGAP_KEYS_FILE_CONTENT)

# The content of the keys file will be a dictionary like this (though not indented):
#   {
#       'fourfront-cgap': {
#           'key': 'key1',
#           'secret': 'somesecret1',
#           'server': 'https://cgap.hms.harvard.edu'
#       },
#       'fourfront-cgapfoo': {
#           'key': 'key2',
#           'secret': 'somesecret2',
#           'server': 'http://fourfront-cgapfoo.whatever.aws.com'
#       },
#       'fourfront-cgaplocal': {
#           'key': 'key3',
#           'secret': 'somesecret3',
#           'server': 'http://localhost:8000'
#       }
#   }


def test_get_keydicts():

    key_manager = _make_sample_cgap_key_manager()
    mfs = MockFileSystem(files={key_manager.keys_file: SAMPLE_CGAP_KEYS_FILE_TEXT})

    with mfs.mock_exists_open_remove():

        assert key_manager.get_keydicts() == SAMPLE_CGAP_KEYS_FILE_CONTENT


def test_get_keypair_for_env():

    key_manager = _make_sample_cgap_key_manager()
    mfs = MockFileSystem(files={key_manager.keys_file: SAMPLE_CGAP_KEYS_FILE_TEXT})

    with mfs.mock_exists_open_remove():

        assert key_manager.get_keypair_for_env(SAMPLE_CGAP_PRODUCTION_ENV) == SAMPLE_CGAP_PAIR
        assert key_manager.get_keypair_for_env(SAMPLE_CGAP_FOO_ENV) == SAMPLE_CGAP_FOO_PAIR
        assert key_manager.get_keypair_for_env(SAMPLE_CGAP_LOCAL_PSEUDOENV) == SAMPLE_CGAP_LOCAL_PAIR

        with pytest.raises(AppEnvKeyMissing):  # If an environment is missing, an error is raised.
            key_manager.get_keypair_for_env(SAMPLE_MISSING_ENV)

        with pytest.raises(ValueError):  # None is not a valid environment name. There is no default.
            key_manager.get_keypair_for_env(None)

        with pytest.raises(Exception):  # Wrong number of arguments. The environment name does not default.
            key_manager.get_keypair_for_env()  # noQA


def test_get_keydict_for_env():

    key_manager = _make_sample_cgap_key_manager()
    mfs = MockFileSystem(files={key_manager.keys_file: SAMPLE_CGAP_KEYS_FILE_TEXT})

    with mfs.mock_exists_open_remove():

        assert key_manager.get_keydict_for_env(SAMPLE_CGAP_PRODUCTION_ENV) == SAMPLE_CGAP_DICT
        assert key_manager.get_keydict_for_env(SAMPLE_CGAP_FOO_ENV) == SAMPLE_CGAP_FOO_DICT
        assert key_manager.get_keydict_for_env(SAMPLE_CGAP_LOCAL_PSEUDOENV) == SAMPLE_CGAP_LOCAL_DICT

        with pytest.raises(AppEnvKeyMissing):  # If an environment is missing, an error is raised.
            key_manager.get_keydict_for_env(SAMPLE_MISSING_ENV)

        with pytest.raises(ValueError):  # None is not a valid environment name. There is no default.
            key_manager.get_keydict_for_env(None)

        with pytest.raises(Exception):  # Wrong number of arguments. The environment name does not default.
            key_manager.get_keydict_for_env()  # noQA


def test_get_keypair_for_server():

    key_manager = _make_sample_cgap_key_manager()
    mfs = MockFileSystem(files={key_manager.keys_file: SAMPLE_CGAP_KEYS_FILE_TEXT})

    with mfs.mock_exists_open_remove():

        assert key_manager.get_keypair_for_server(SAMPLE_CGAP_PRODUCTION_SERVER) == SAMPLE_CGAP_PAIR
        assert key_manager.get_keypair_for_server(SAMPLE_CGAP_FOO_SERVER) == SAMPLE_CGAP_FOO_PAIR
        assert key_manager.get_keypair_for_server(SAMPLE_CGAP_LOCAL_SERVER) == SAMPLE_CGAP_LOCAL_PAIR

        with pytest.raises(AppServerKeyMissing):
            key_manager.get_keypair_for_server(SAMPLE_MISSING_SERVER)

        with pytest.raises(ValueError):  # None is not a valid server name. There is no default.
            key_manager.get_keypair_for_server(None)

        with pytest.raises(Exception):  # wrong number of arguments
            key_manager.get_keypair_for_server()  # noQA


def test_get_keydict_for_server():

    key_manager = _make_sample_cgap_key_manager()
    mfs = MockFileSystem(files={key_manager.keys_file: SAMPLE_CGAP_KEYS_FILE_TEXT})

    with mfs.mock_exists_open_remove():

        assert key_manager.get_keydict_for_server(SAMPLE_CGAP_PRODUCTION_SERVER) == SAMPLE_CGAP_DICT
        assert key_manager.get_keydict_for_server(SAMPLE_CGAP_FOO_SERVER) == SAMPLE_CGAP_FOO_DICT
        assert key_manager.get_keydict_for_server(SAMPLE_CGAP_LOCAL_SERVER) == SAMPLE_CGAP_LOCAL_DICT

        with pytest.raises(AppServerKeyMissing):
            key_manager.get_keydict_for_server(SAMPLE_MISSING_SERVER)

        with pytest.raises(ValueError):  # None is not a valid server name. There is no default.
            key_manager.get_keydict_for_server(None)

        with pytest.raises(Exception):  # wrong number of arguments
            key_manager.get_keydict_for_server()  # noQA
