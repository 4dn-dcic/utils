import json
import os

import pytest

from dcicutils import secrets_utils as secrets_utils_module
from dcicutils.misc_utils import override_environ, ignored
from dcicutils.qa_utils import MockBoto3
from dcicutils.secrets_utils import (
    GLOBAL_APPLICATION_CONFIGURATION, LEGACY_DEFAULT_IDENTITY_NAME,
    get_identity_name, identity_is_defined, apply_overrides,
    assumed_identity, assumed_identity_if, apply_identity,
    get_identity_secrets, assume_identity, SecretsTable,
)
from unittest import mock


some_secret_name_1 = 'foo12345'
some_secret_name_2 = 'bar67890'

some_secret_names = [some_secret_name_1, some_secret_name_2]

some_secret_values = ['the {item} thing' for item in some_secret_names]

some_secret_value_1 = some_secret_values[0]
some_secret_value_2 = some_secret_values[1]

# in other words, some_secret_table = {"foo": "the foo thing", "bar": "the bar thing"}
some_secret_table = dict(zip(some_secret_names, some_secret_values))
some_secret_string = json.dumps(some_secret_table)
# The common identity pattern is for use in tests that want to find several things that have the same common substring,
# but that don't match the full set.
some_common_identity_substring = "SecretIdentityToken"
some_common_identity_pattern = "Secret.*Token"
some_unique_secret_identity_token = "Mastertest"
some_secret_identity = (f'Mocked{some_unique_secret_identity_token}'
                        f'ApplicationConfiguration{some_common_identity_substring}')
# These decoys are because sometimes a SecretsManager has more than just an application configuration in it.
# For example, 4dn-cloud-infra separately manages the secrets associated with RDS into other data that could
# likewise be accessed via a SecretsTable.  Testing assumes that we're lookning for an ApplicationConfiguration,
# but that's an arbitrary choice.
decoy_1_string = '{"decoy": 1}'
decoy_1_identity = f'MockedDecoy1Something{some_common_identity_substring}'
decoy_2_string = '{"decoy": 2}'
# This search string will find exactly one secret that is not our application configuration for testing.
some_unique_decoy_token = "UniqueThing"
# This search string will find exactly one secret that is not our application configuration for testing.
some_unique_decoy_name = f'MockedDecoy2{some_unique_decoy_token}'
decoy_2_identity = some_unique_decoy_name
some_secret_identities_with_common_pattern = [some_secret_identity, decoy_1_identity]
some_secret_identities = [some_secret_identity, decoy_1_identity, decoy_2_identity]


def test_get_identity_name():

    with override_environ(**{GLOBAL_APPLICATION_CONFIGURATION: None}):
        assert get_identity_name() == LEGACY_DEFAULT_IDENTITY_NAME
        assert get_identity_name('MY_IDENTITY') == LEGACY_DEFAULT_IDENTITY_NAME

    some_name = 'SomeIdentityForTesting'
    with override_environ(**{GLOBAL_APPLICATION_CONFIGURATION: some_name}):
        assert get_identity_name() == some_name
        assert get_identity_name('MY_IDENTITY') == LEGACY_DEFAULT_IDENTITY_NAME


def test_identity_is_defined():

    with override_environ(**{GLOBAL_APPLICATION_CONFIGURATION: None}):
        assert identity_is_defined() is False
        assert identity_is_defined('IDENTITY') is False
        assert identity_is_defined('MY_IDENTITY') is False

    some_name = 'SomeIdentityForTesting'
    with override_environ(**{GLOBAL_APPLICATION_CONFIGURATION: some_name}):
        assert identity_is_defined() is True
        assert identity_is_defined('IDENTITY') is True
        assert identity_is_defined('MY_IDENTITY') is False

    with override_environ(MY_IDENTITY='SomeOtherIdentity'):
        assert identity_is_defined('MY_IDENTITY') is True


def test_apply_overrides():

    secrets = {'x': 1, 'y': 2}

    assert apply_overrides(secrets=secrets, override_values=None) == secrets
    assert apply_overrides(secrets=secrets, override_values={}) == secrets
    assert apply_overrides(secrets=secrets) == secrets

    assert apply_overrides(secrets=secrets, rename_keys={'x': 'ex'}) == {'ex': 1, 'y': 2}
    assert apply_overrides(secrets=secrets, rename_keys={'x': 'ex', 'y': 'why'}) == {'ex': 1, 'why': 2}

    with pytest.raises(ValueError):
        apply_overrides(secrets=secrets, rename_keys={'z': 'zee'})

    with pytest.raises(ValueError):
        apply_overrides(secrets=secrets, rename_keys={'x': 'y'})

    assert apply_overrides(secrets=secrets, override_values={'x': 3}) == {'x': 3, 'y': 2}
    assert apply_overrides(secrets=secrets, override_values={'x': 3, 'z': 9}) == {'x': 3, 'y': 2, 'z': 9}

    assert (apply_overrides(secrets=secrets, rename_keys={'x': 'ex', 'y': 'why'}, override_values={'ex': 3, 'zee': 9})
            == {'ex': 3, 'why': 2, 'zee': 9})

    assert (apply_overrides(secrets=secrets, rename_keys={'x': 'ex', 'y': 'why'}, override_values={'x': 3, 'zee': 9})
            == {'ex': 1, 'why': 2, 'zee': 9, 'x': 3})


def boto3_for_some_secrets_testing():
    mocked_boto3 = MockBoto3()
    manager = mocked_boto3.client('secretsmanager')
    manager.put_secret_value_for_testing(SecretId=some_secret_identity, Value=some_secret_string)
    manager.put_secret_value_for_testing(SecretId=decoy_1_identity, Value=decoy_1_string)
    manager.put_secret_value_for_testing(SecretId=decoy_2_identity, Value=decoy_2_string)
    return mocked_boto3


def test_get_identity_secrets():
    check_get_identity_secrets(get_identity_secrets)


def test_assume_identity():
    check_get_identity_secrets(assume_identity)


def check_get_identity_secrets(fn):

    b3 = boto3_for_some_secrets_testing()  # this sets things up with some_secret_string in the SecretsManager
    with mock.patch.object(secrets_utils_module, 'boto3', b3):
        with override_environ(IDENTITY=some_secret_identity):
            secrets = fn()
            assert secrets == some_secret_table  # this is the parsed form of some_secret_string


def test_assumed_identity():

    b3 = boto3_for_some_secrets_testing()
    with mock.patch.object(secrets_utils_module, 'boto3', b3):

        for i in range(len(some_secret_names)):
            secret_name = some_secret_names[i]
            # Check that this test will be useful.
            assert secret_name not in os.environ, f"The test secret variable {secret_name} is already in os.environ."

        with override_environ(IDENTITY=some_secret_identity):
            with assumed_identity():
                for i in range(len(some_secret_names)):
                    secret_name = some_secret_names[i]
                    secret_value = some_secret_values[i]
                    assert secret_name in os.environ, f"The test secret variable {secret_name} is missing in os.environ."
                    assert os.environ[secret_name] == secret_value

        with override_environ(MY_IDENTITY=some_secret_identity):
            with assumed_identity(identity_kind='MY_IDENTITY'):
                for i in range(len(some_secret_names)):
                    secret_name = some_secret_names[i]
                    secret_value = some_secret_values[i]
                    assert secret_name in os.environ, f"The test secret variable {secret_name} is missing in os.environ."
                    assert os.environ[secret_name] == secret_value

        alt_name_1 = f"alt_{some_secret_name_1}"
        alt_secret_names = [alt_name_1] + some_secret_names[1:]
        alt_secret_values = some_secret_values

        with override_environ(MY_IDENTITY=some_secret_identity):
            with assumed_identity(identity_kind='MY_IDENTITY', rename_keys={some_secret_name_1: alt_name_1}):
                for i in range(len(alt_secret_names)):
                    alt_name = alt_secret_names[i]
                    alt_value = alt_secret_values[i]
                    assert alt_name in os.environ, f"The test secret variable {alt_name} is missing in os.environ."
                    assert os.environ[secret_name] == alt_value

        some_overridden_value = 'some other value'
        with override_environ(IDENTITY=some_secret_identity):
            with assumed_identity(override_values={some_secret_name_1: some_overridden_value}):
                assert os.environ[some_secret_name_1] == some_overridden_value
                assert os.environ[some_secret_name_2] == some_secret_value_2

        with override_environ(IDENTITY=some_secret_identity):
            # This will assume the identity because some_secret_name_2 is not bound in os.environ.
            with assumed_identity(only_if_missing=some_secret_name_2):
                assert some_secret_name_1 in os.environ

        with override_environ(IDENTITY=some_secret_identity, **{some_secret_name_1: 'anything'}):
            # This will assume the identity because some_secret_name_2 is not bound in os.environ.
            # (Note here that some secret names are present in the environment, but not the right one.)
            with assumed_identity(only_if_missing=some_secret_name_2):
                assert some_secret_name_1 in os.environ

        with override_environ(IDENTITY=some_secret_identity, **{some_secret_name_2: 'anything'}):
            # This will NOT assume the identity because some_secret_name_2 IS bound in os.environ.
            with assumed_identity(only_if_missing=some_secret_name_2):
                assert some_secret_name_1 not in os.environ

def test_assumed_identity_if():

    b3 = boto3_for_some_secrets_testing()
    with mock.patch.object(secrets_utils_module, 'boto3', b3):

        for i in range(len(some_secret_names)):
            secret_name = some_secret_names[i]
            # Check that this test will be useful.
            assert secret_name not in os.environ, f"The test secret variable {secret_name} is already in os.environ."

        with override_environ(IDENTITY=some_secret_identity):

            with assumed_identity_if(True):
                assert some_secret_name_1 in os.environ

            with assumed_identity_if(False):
                assert some_secret_name_1 not in os.environ


def test_secrets_table_as_dict():

    b3 = boto3_for_some_secrets_testing()
    with mock.patch.object(secrets_utils_module, 'boto3', b3):
        # Note that unlike get_identity_secrets, we don't need an IDENTITY env variable,
        # because this strategy is assuming a single applicable secret.
        secrets_table = SecretsTable.find_application_secrets_table()
        identity = secrets_table.as_dict()
        assert identity == some_secret_table


def test_mock_secrets_table_as_dict_and_get_identity_secrets_equivalence():

    b3 = boto3_for_some_secrets_testing()
    with mock.patch.object(secrets_utils_module, 'boto3', b3):
        with override_environ(IDENTITY=some_secret_identity):
            identity = get_identity_secrets()
            secret_table = SecretsTable.find_application_secrets_table()
            secret_table_as_dict = secret_table.as_dict()
            assert identity == secret_table_as_dict == some_secret_table


def test_mock_secrets_table_str_and_repr():

    with mock.patch.object(secrets_utils_module, "id") as mocked_id:
        # We can't control the actual memory address to test the printer, so we have to mock 'id' to lie about it.
        some_id = 12345678  # this will be the pretend address of our object
        hex_id = "%x" % some_id
        assert hex_id == 'bc614e'
        mocked_id.return_value = some_id
        some_name = "some_name"
        str_form = str(SecretsTable(name=some_name))
        assert str_form == "<dcicutils.secrets_utils.SecretsTable 'some_name' @bc614e>"
        repr_form = repr(SecretsTable(name=some_name))
        assert repr_form == "<dcicutils.secrets_utils.SecretsTable 'some_name' @bc614e>"


def test_secrets_table_get_secret_value():

    b3 = boto3_for_some_secrets_testing()
    with mock.patch.object(secrets_utils_module, 'boto3', b3):
        secrets_table = SecretsTable.find_application_secrets_table()
        value = secrets_table._get_secret_value()
        assert isinstance(value, dict)
        assert value['SecretString'] == some_secret_string


def test_secrets_table_get_secret_string():

    b3 = boto3_for_some_secrets_testing()
    with mock.patch.object(secrets_utils_module, 'boto3', b3):
        secrets_table = SecretsTable.find_application_secrets_table()
        secret_string = secrets_table._get_secret_string()
        assert secret_string == some_secret_string


def test_secrets_table_get():

    b3 = boto3_for_some_secrets_testing()
    with mock.patch.object(secrets_utils_module, 'boto3', b3):
        secrets_table = SecretsTable.find_application_secrets_table()
        secret_foo = secrets_table.get(some_secret_name_1)
        assert secret_foo == some_secret_table[some_secret_name_1]
        secret_missing = secrets_table.get('missing')
        assert secret_missing is None


def test_secrets_table_getitem():

    b3 = boto3_for_some_secrets_testing()
    with mock.patch.object(secrets_utils_module, 'boto3', b3):
        secrets_table = SecretsTable.find_application_secrets_table()
        secret_foo = secrets_table[some_secret_name_1]
        assert secret_foo == some_secret_table[some_secret_name_1]
        with pytest.raises(KeyError):
            secret_missing = secrets_table['missing']
            ignored(secret_missing)  # the previous line will fail


def test_secrets_table_internal_all_secrets():

    b3 = boto3_for_some_secrets_testing()

    with mock.patch.object(secrets_utils_module, 'boto3', b3):
        secretsmanager_client = b3.client('secretsmanager')
        all_secrets = SecretsTable._all_secrets(secretsmanager_client=secretsmanager_client)
        all_secret_names = [secret['Name'] for secret in all_secrets]
        assert all_secret_names == some_secret_identities

        # We can also fetch the client to use from another SecretsTable
        secretsmanager_client = SecretsTable.find_application_secrets_table().secretsmanager_client
        all_secrets = SecretsTable._all_secrets(secretsmanager_client=secretsmanager_client)
        all_secret_names = [secret['Name'] for secret in all_secrets]
        assert all_secret_names == some_secret_identities


def test_secrets_table_internal_find_secrets_using_substring():

    b3 = boto3_for_some_secrets_testing()
    with mock.patch.object(secrets_utils_module, 'boto3', b3):
        secretsmanager_client = b3.client('secretsmanager')
        found_secrets = SecretsTable._find_secrets(pattern=some_common_identity_substring,
                                                   secretsmanager_client=secretsmanager_client)
        assert isinstance(found_secrets, list)
        assert len(found_secrets) > 0
        assert all(isinstance(found_secret, dict) for found_secret in found_secrets)
        found_secret_names = [secret['Name'] for secret in found_secrets]
        print(f"found_secret_names={found_secret_names}")
        print(f"some_secret_identities_with_common_pattern={some_secret_identities_with_common_pattern}")
        assert found_secret_names == some_secret_identities_with_common_pattern


def test_secrets_table_internal_find_secrets_pattern():

    b3 = boto3_for_some_secrets_testing()
    with mock.patch.object(secrets_utils_module, 'boto3', b3):
        secretsmanager_client = b3.client('secretsmanager')
        found_secrets = SecretsTable._find_secrets(pattern=some_common_identity_pattern,
                                                   secretsmanager_client=secretsmanager_client)
        assert isinstance(found_secrets, list)
        assert len(found_secrets) > 0
        assert all(isinstance(found_secret, dict) for found_secret in found_secrets)
        found_secret_names = [secret['Name'] for secret in found_secrets]
        assert found_secret_names == some_secret_identities_with_common_pattern


def test_secrets_table_internal_find_secret():

    b3 = boto3_for_some_secrets_testing()
    with mock.patch.object(secrets_utils_module, 'boto3', b3):
        secretsmanager_client = b3.client('secretsmanager')
        found_secret = SecretsTable._find_secret(pattern=some_unique_decoy_token,
                                                 secretsmanager_client=secretsmanager_client)
        assert isinstance(found_secret, dict)
        found_secret_name = found_secret['Name']
        assert found_secret_name == some_unique_decoy_name

        with pytest.raises(RuntimeError):  # Error if too many matches
            SecretsTable._find_secret(pattern=some_common_identity_pattern,
                                      secretsmanager_client=secretsmanager_client)

        with pytest.raises(RuntimeError):  # Error if too few matches
            SecretsTable._find_secret(pattern="this will not match",
                                      secretsmanager_client=secretsmanager_client)


def test_secrets_table_find_secret():

    b3 = boto3_for_some_secrets_testing()
    with mock.patch.object(secrets_utils_module, 'boto3', b3):
        secrets_table = SecretsTable.find_application_secrets_table()
        # The '_alt' thing here is us testing we can call with or without a secretsmanager_client.
        # The choice is purely a matter of reusing resources for efficiency.
        found_secret = SecretsTable.find_secret(pattern=some_unique_decoy_token)
        found_secret_alt = SecretsTable.find_secret(secretsmanager_client=secrets_table.secretsmanager_client,
                                                    pattern=some_unique_decoy_token)
        assert found_secret == found_secret_alt
        assert isinstance(found_secret, dict)
        found_secret_name = found_secret['Name']
        assert found_secret_name == some_unique_decoy_name

        with pytest.raises(RuntimeError):  # Error if too many matches
            SecretsTable.find_secret(pattern=some_common_identity_pattern)

        with pytest.raises(RuntimeError):  # Error if too few matches
            SecretsTable.find_secret(pattern="this will not match")


def test_find_application_secrets_table():

    b3 = boto3_for_some_secrets_testing()
    with mock.patch.object(secrets_utils_module, 'boto3', b3):

        found_table = SecretsTable.find_application_secrets_table()  # in our examples, there's only one
        assert found_table.name == some_secret_identity

        found_table = SecretsTable.find_application_secrets_table(
            application_configuration_pattern=some_unique_secret_identity_token)
        assert found_table.name == some_secret_identity

        with pytest.raises(RuntimeError):  # there are too many for this to succeed unassisted
            # There is no thing called DecoyApplicationConfiguration
            found = SecretsTable.find_application_secrets_table(application_configuration_pattern='Decoy')
            ignored(found)  # Shouldn't reach here
