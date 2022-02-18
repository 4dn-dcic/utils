import json
import pytest

from dcicutils import secrets_utils as secrets_utils_module
from dcicutils.misc_utils import override_environ, ignored
from dcicutils.qa_utils import MockBoto3
from dcicutils.secrets_utils import assume_identity, SecretsTable
from unittest import mock


some_secret_names = ['foo', 'bar']
some_secret_values = ['the {item} thing' for item in some_secret_names]
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


def boto3_for_some_secrets_testing():
    mocked_boto3 = MockBoto3()
    manager = mocked_boto3.client('secretsmanager')
    manager.put_secret_value_for_testing(SecretId=some_secret_identity, Value=some_secret_string)
    manager.put_secret_value_for_testing(SecretId=decoy_1_identity, Value=decoy_1_string)
    manager.put_secret_value_for_testing(SecretId=decoy_2_identity, Value=decoy_2_string)
    return mocked_boto3


def test_assume_identity():

    b3 = boto3_for_some_secrets_testing()  # this sets things up with some_secret_string in the SecretsManager
    with mock.patch.object(secrets_utils_module, 'boto3', b3):
        with override_environ(IDENTITY=some_secret_identity):
            identity = assume_identity()
            assert identity == some_secret_table  # this is the parsed form of some_secret_string


def test_secrets_table_as_dict():

    b3 = boto3_for_some_secrets_testing()
    with mock.patch.object(secrets_utils_module, 'boto3', b3):
        # Note that unlike assume_identity, we don't need an IDENTITY env variable,
        # because this strategy is assuming a single applicable secret.
        secrets_table = SecretsTable.find_application_secrets_table()
        identity = secrets_table.as_dict()
        assert identity == some_secret_table


def test_mock_secrets_table_as_dict_and_assume_identity_equivalence():

    b3 = boto3_for_some_secrets_testing()
    with mock.patch.object(secrets_utils_module, 'boto3', b3):
        with override_environ(IDENTITY=some_secret_identity):
            identity = assume_identity()
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
        secret_foo = secrets_table.get('foo')
        assert secret_foo == some_secret_table['foo']
        secret_missing = secrets_table.get('missing')
        assert secret_missing is None


def test_secrets_table_getitem():

    b3 = boto3_for_some_secrets_testing()
    with mock.patch.object(secrets_utils_module, 'boto3', b3):
        secrets_table = SecretsTable.find_application_secrets_table()
        secret_foo = secrets_table['foo']
        assert secret_foo == some_secret_table['foo']
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
