import contextlib
import os
import boto3
import json
import re
import structlog

from botocore.exceptions import ClientError
from typing import Optional
from .common import REGION as COMMON_REGION
from .lang_utils import a_or_an
from .misc_utils import PRINT, full_class_name, override_environ


logger = structlog.getLogger(__name__)


# Environment variable whose value is the name of a secret
# stored in AWS Secrets Manager that contains the global
# application configuration. This secret JSON is acquired
# and returned to the caller. Note that this API can behave differently
# across AWS Accounts.
GLOBAL_APPLICATION_CONFIGURATION = 'IDENTITY'


# TODO: Rethink whether this should be a constant. Maybe it should be a function of the identity_kind?
#       Maybe the caller should pass a default and this function should have none. -kmp 18-Jul-2022
LEGACY_DEFAULT_IDENTITY_NAME = 'dev/beanstalk/cgap-dev'


def get_identity_name(identity_kind: str = GLOBAL_APPLICATION_CONFIGURATION):
    return os.environ.get(identity_kind, LEGACY_DEFAULT_IDENTITY_NAME)


def identity_is_defined(identity_kind: str = GLOBAL_APPLICATION_CONFIGURATION) -> bool:
    return bool(os.environ.get(identity_kind))


def get_identity_secrets(identity_kind: str = GLOBAL_APPLICATION_CONFIGURATION) -> dict:
    """ Grabs application identity from the secrets manager.
        Looks for environment variable IDENTITY, which should contain the name of
        a secret in secretsmanager that is a JSON blob of core configuration information.
        Default value is current value in the test account. This name should be the
        name of the environment.
    """
    secret_name = get_identity_name(identity_kind)
    region_name = COMMON_REGION  # us-east-1, must match ECR/ECS Region, which also imports its default from .common
    session = boto3.session.Session(region_name=region_name)
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:  # leaving some useful debug info to help narrow issues
        if e.response['Error']['Code'] in [
            'DecryptionFailureException'  # SM can't decrypt the protected secret text using the provided KMS key.
            'InternalServiceErrorException',  # An error occurred on the server side.
            'InvalidParameterException',  # You provided an invalid value for a parameter.
            'InvalidRequestException',  # Invalid parameter value for the current state of the resource.
            'ResourceNotFoundException',
        ]:
            PRINT('Encountered a known exception trying to acquire secrets.')
        raise e
    else:
        # Decrypts secret using the associated KMS CMK.
        # Depending on whether the secret is a string or binary, one of these fields will be populated.
        if 'SecretString' in get_secret_value_response:
            identity_secrets = json.loads(get_secret_value_response['SecretString'])
        else:
            raise Exception('Got unexpected response structure from boto3')
    if not identity_secrets:
        raise Exception('Identity could not be found! Check the secret name.')
    elif not isinstance(identity_secrets, dict):
        raise Exception("Identity was not in expected dictionary form.")
    return identity_secrets


assume_identity = get_identity_secrets  # assume_identity is deprecated. Please rewrite


def apply_overrides(*, secrets: dict, rename_keys: Optional[dict] = None,
                    override_values: Optional[dict] = None) -> dict:
    if rename_keys:
        secrets = secrets.copy()
        for key, new_name in rename_keys.items():
            if new_name in secrets:
                raise ValueError(f"Cannot rename {key} to {new_name}"
                                 f" because {a_or_an(new_name)} attribute already exists.")
            if key not in secrets:
                raise ValueError(f"Cannot rename {key} to {new_name} because the secrets has no {key} attribute.")
            secrets[new_name] = secrets.pop(key)
    if override_values:
        secrets = dict(secrets, **override_values)
    return secrets


@contextlib.contextmanager
def assumed_identity_if(only_if: bool, *,
                        identity_kind: str = GLOBAL_APPLICATION_CONFIGURATION,
                        only_if_missing: Optional[str] = None,
                        require_identity: bool = False,
                        rename_keys: Optional[dict] = None,
                        override_values: Optional[dict] = None):
    with assumed_identity(identity_kind=identity_kind, only_if=only_if, only_if_missing=only_if_missing,
                          require_identity=require_identity, rename_keys=rename_keys, override_values=override_values):
        yield


@contextlib.contextmanager
def assumed_identity(*,
                     identity_kind: str = GLOBAL_APPLICATION_CONFIGURATION,
                     only_if: bool = True,
                     only_if_missing: Optional[str] = None,
                     require_identity: bool = False,
                     rename_keys: Optional[dict] = None,
                     override_values: Optional[dict] = None):
    """
    Assumes a given identity in a context.

    The rename_keys happen before the override_values.

    >>> apply_overrides(secrets={'x': 1, 'y': 2}, rename_keys={'x': 'ex', 'y': 'why'}, override_values={'x': 3, 'z': 9})
    {'ex': 1, 'why': 2, 'z': 9, 'x': 3}

    NOTE: This assumes that global_env_bucket is in the GAC and not otherwise,
          so it tests

    :param identity_kind: The name of the identity to assume (default 'IDENTITY')
    :param only_if: Whether to try assuming identity at all (default True)
    :param only_if_missing: The name of an environment variable that would only be in the GAC.
       Load the GAC only if this variable is not set. Default is None, meaning load unconditionally.
    :param require_identity: Whether to raise an error if the identity_kind environment variable is not bound.
    :param rename_keys: If present, a dictionary mapping keys to override keys. Default None.
    :param override_values: If present, a dictionary mapping keys to override values. Default None.

    """
    if only_if and (not only_if_missing or not os.environ.get(only_if_missing)):
        identity_name = get_identity_name(identity_kind=identity_kind)
        if identity_name:
            secrets = get_identity_secrets(identity_kind=identity_kind)
            secrets = apply_overrides(secrets=secrets, rename_keys=rename_keys, override_values=override_values)
            if only_if_missing and only_if_missing not in secrets:
                raise RuntimeError(f"No {only_if_missing} was found where expected"
                                   f" in {identity_kind} secrets at {identity_name}.")
            with override_environ(**secrets):
                yield
                return  # Nothing to do after that

        elif require_identity:
            raise RuntimeError(f"An identity was not defined in environment variable {identity_kind}.")

    yield


def apply_identity(identity_kind: str = GLOBAL_APPLICATION_CONFIGURATION,
                   rename_keys: Optional[dict] = None, override_values: Optional[dict] = None):
    """
    Assumes an identity globally by assigning environment variables.

    Since this is assumed to be an initial startup action, we assume it's not already been done and don't
    offer the only_if_missing mechanism that the assumed_identity context manager offers.

    """
    if identity_is_defined(identity_kind):
        secrets = get_identity_secrets(identity_kind=identity_kind)
        secrets = apply_overrides(secrets=secrets, rename_keys=rename_keys, override_values=override_values)
        for var, val in secrets.items():
            os.environ[var] = val
    else:
        raise RuntimeError(f"An identity was not defined in environment variable {identity_kind}.")


# Some hints about how to manipulate secrets are here:
# https://docs.aws.amazon.com/code-samples/latest/catalog/python-secretsmanager-secretsmanager_basics.py.html
# However, the examples are poorly written and involve way too much weird state. -kmp 16-Feb-2022

class SecretsTable:
    """
    Assists with certain read-only operations on a given secret in the SecertsManager.

    This can be created with the name of what the AWS SecretsManager calls a secret,
    and what we call an identity. The idea is to allow read-only access to that data
    in tabular form.

    Once created, this table can be accessed via .as_dict() to turn it into a regular
    dictionary, but it can also be accessed via secrets_table.get(entryname) or
    secretstable[entryname] to access individual entries in the table. See tests for examples.

    NOTE: We could later extend this to allow creating or modifying the table as well,
    but that might involve extra permissions and be less broadly useful.
    """

    REGION = COMMON_REGION

    def __init__(self, name, secretsmanager_client=None, stage=None, region=None):
        """
        :param name: The SecretId of the secret to help with.
        :param secretsmanager_client: A Boto3 Secrets Manager client to use. If unsupplied, one is created.
        """
        if not str or not isinstance(name, str):
            raise ValueError(f"Bad secret name: {name}")
        self.secretsmanager_client = (secretsmanager_client
                                      or boto3.client('secretsmanager', region_name=region or self.REGION))
        self.name = name
        self.stage = stage

    def __str__(self):
        """
        Returns a string summarizing the table by name.

        :return: a string for use by str to describe this object as <SecretsTable 'name' @nnnn>
        """
        return f"<{full_class_name(self)} {self.name!r} @{'%x' % id(self)}>"

    def __repr__(self):
        """
        Returns a non-rereadable string summarizing the table by name.

        :return: a string for use by repr to describe this object as <SecretsTable 'name' @nnnn>,
                 the same as str does.
        """
        return self.__str__()

    def _get_secret_value(self):
        """
        Gets the 'value' of a secret in the SecretsManager.

        Even once a string is obtained, it may be the string containing the printed representation of a dictionary,
        so it may be more useful to use .get_secret_dict or to use .get or [...].

        :return: The value of the secret. When the secret is a string, the value is
                 contained in the `SecretString` field. When the secret is bytes,
                 it is contained in the `SecretBinary` field.
        """
        try:
            kwargs = {'SecretId': self.name}
            if self.stage is not None:
                kwargs['VersionStage'] = self.stage
            response = self.secretsmanager_client.get_secret_value(**kwargs)
        except ClientError as e:
            logger.error(f"Unable to retrieve value for secret {self.name}. {type(e)}: {e}")
            raise
        else:
            return response

    def _get_secret_string(self):
        """
        The secrets held by the AWS SecretsManager are actually strings or binary blobs.
        The strings are often JSON representing detailed entries in a table. This just
        gets the big string, in the case that it's string data (the only case we care about)
        so is mostly an internal thing. No user should be calling this.

        Note that this MIGHT return None if there is no string secret, usually because there was
        binary data instead. We expect that in our case there always will be string data in SecretString,
        but it would be in SecretBinary instead if there was binary data.
        """
        result = self._get_secret_value().get('SecretString')
        if result is None:
            raise ValueError(f"There is no SecretString for named {self.name!r}.")
        return result

    def as_dict(self):
        """
        Returns a representation of the secret as a Python dict.

        Note that calling this function gives the equivalent to what secrets_utils.assume_identity returns.
        See the tests for an example.

        :return: a dict representing the secrets table
        """
        secret_string = self._get_secret_string()
        if secret_string:
            try:
                return json.loads(secret_string)
            except Exception:
                raise ValueError(f"Unable to parse SecretString named {self.name} as a dictionary.")

    def get(self, item, default=None):
        """
        Returns the value of a given entry in the SecretsTable, or a default value (default None) otherwise.

        :item: a string naming the entry.
        :default: a default to use if the entry is missing (default None)

        :return: a string or None
        """
        return self.as_dict().get(item, default)

    def __getitem__(self, item):
        """
        Returns the value of a given entry in the SecretsTable, which is presumed to exist (or an error is raised).

        :item: a string naming the entry.

        :return: a string

        :raises KeyError: if the entry does not exist
        """
        result = self.get(item)
        if not result:
            raise KeyError(item)
        else:
            return result

    @classmethod
    def _all_secrets(cls, *, secretsmanager_client):
        """
        The AWS SecretsManager is itself a table. This gets a list of metadata dictionaries for each item in the
        SecretsManager, each of which can be the name of what we're here calling a SecretsTable. The name of
        each such metadata dictionary, the name of the associated secret, is in the dict entry called 'Name'.

        :return: a list of metadata dictionaries
        """
        return secretsmanager_client.list_secrets()['SecretList']

    @classmethod
    def _find_secrets(cls, *, pattern, secretsmanager_client):
        """
        This is like ._all_secrets() but returns a list of metadata dictionaries that correspond only to those
        whose names match the given regexp pattern.

        :return: a list of metadata dictionaries
        """
        results = []
        for secret in cls._all_secrets(secretsmanager_client=secretsmanager_client):
            if re.search(pattern, secret['Name']):
                results.append(secret)
        return results

    @classmethod
    def _find_secret(cls, *, pattern, secretsmanager_client):
        """
        This is like ._all_secrets() but returns exactly one metadata dictionary (not in a list)
        that has a name matching the given regexp pattern. It is an error if there are too many
        or none at all.

        :return: a metadata dictionary

        :raises RuntimeError: if zero matching dictionaries or more than one matching dictionaries are found
        """
        results = cls._find_secrets(secretsmanager_client=secretsmanager_client, pattern=pattern)
        n = len(results)
        if n == 0:
            raise RuntimeError(f"No secret was found matching {pattern!r}.")
        elif n == 1:
            return results[0]
        else:
            raise RuntimeError(f"Too many secrets match {pattern!r}: {[result['Name'] for result in results]}")

    @classmethod
    def find_secret(cls, pattern, secretsmanager_client=None):
        """
        This returns metadata about a secret with a name matching the given pattern.

        NOTE: This is like ._find_secret() but is not subprimitive. It is intended for calling by client interfaces,
        so it has nicer defaulting of arguments.

        :return: a metadata dictionary

        :raises RuntimeError: if zero matching dictionaries or more than one matching dictionaries are found
        """

        secretsmanager_client = secretsmanager_client or boto3.client('secretsmanager')
        return cls._find_secret(secretsmanager_client=secretsmanager_client, pattern=pattern)

    @classmethod
    def find_application_secrets_table(cls, secretsmanager_client=None,
                                       application_configuration_pattern='ApplicationConfiguration'):
        """
        This tries to find a secret representing an ApplicationConfiguration, since often there is only one.
        If there are multiple ApplicationConfigurations present, it may be necessary to specify a more specific
        search pattern via application_configuration_pattern=.

        :param secretsmanager_client: A boto3 client for secretsmanager. If one is not suppllied, it will be created.
           But it is sometimes good to reuse an existing one if you have one handy.
        :param application_configuration_pattern: A search pattern to use if there is intended to be be more than one
           application configuration to select among. The default should be fine if there is only one, although
           this somewhat depends on how 4dn-cloud-infra orchestration names things.

        :return: a SecretsTable representing the SecretString in the secret that is found by the search
        """
        secretsmanager_client = secretsmanager_client or boto3.client('secretsmanager')
        secret = cls.find_secret(pattern=application_configuration_pattern,
                                 secretsmanager_client=secretsmanager_client)
        secret_name = secret['Name']
        return SecretsTable(name=secret_name, secretsmanager_client=secretsmanager_client)
