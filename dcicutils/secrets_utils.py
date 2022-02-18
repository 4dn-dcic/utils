import os
import boto3
import json
import re
import structlog

from botocore.exceptions import ClientError
from .ecr_utils import CGAP_ECR_REGION
from .misc_utils import PRINT, full_class_name


logger = structlog.getLogger(__name__)


# Environment variable whose value is the name of a secret
# stored in AWS Secrets Manager that contains the global
# application configuration. This secret JSON is acquired
# and returned to the caller. Note that this API can behave differently
# across AWS Accounts.
GLOBAL_APPLICATION_CONFIGURATION = 'IDENTITY'


def assume_identity():
    """ Grabs application identity from the secrets manager.
        Looks for environment variable IDENTITY, which should contain the name of
        a secret in secretsmanager that is a JSON blob of core configuration information.
        Default value is current value in the test account. This name should be the
        name of the environment.
    """
    secret_name = os.environ.get(GLOBAL_APPLICATION_CONFIGURATION, 'dev/beanstalk/cgap-dev')
    region_name = CGAP_ECR_REGION  # us-east-1, must match ECR/ECS Region
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
            identity = json.loads(get_secret_value_response['SecretString'])
        else:
            raise Exception('Got unexpected response structure from boto3')
    if not identity:
        raise Exception('Identity could not be found! Check the secret name.')
    return identity


# Some hints about how to manipulate secrets are here:
# https://docs.aws.amazon.com/code-samples/latest/catalog/python-secretsmanager-secretsmanager_basics.py.html
# However, the examples are poorly written and involve way too much weird state. -kmp 16-Feb-2022

class SecretsTable:
    """Assists with certain read-only operations on a given secret in the SecertsManager."""
    def __init__(self, name, secretsmanager_client=None, stage=None):
        """
        :param name: The SecretId of the secret to help with.
        :param secretsmanager_client: A Boto3 Secrets Manager client to use. If unsupplied, one is created.
        """
        if not str or not isinstance(name, str):
            raise ValueError(f"Bad secret name: {name}")
        self.secretsmanager_client = (secretsmanager_client
                                      or boto3.client('secretsmanager', region_name=CGAP_ECR_REGION))
        self.name = name
        self.stage = stage

    def __str__(self):
        return f"<{full_class_name(self)} {self.name!r} @{'%x' % id(self)}>"

    def __repr__(self):
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
        # This might return None if there is no string secret. We exepct that in o ur case there always will be.
        # Note that binary secrets are in SecretBinary instead of SecretString, but we don't use those.
        result = self._get_secret_value().get('SecretString')
        if result is None:
            raise ValueError(f"There is no SecretString for named {self.name!r}.")
        return result

    def as_dict(self):
        secret_string = self._get_secret_string()
        if secret_string:
            try:
                return json.loads(secret_string)
            except Exception:
                raise ValueError(f"Unable to parse SecretString named {self.name} as a dictionary.")

    def get(self, item, default=None):
        return self.as_dict().get(item, default)

    def __getitem__(self, item):
        result = self.get(item)
        if not result:
            raise KeyError(item)
        else:
            return result

    @classmethod
    def _all_secrets(cls, *, secretsmanager_client):
        return secretsmanager_client.list_secrets()['SecretList']

    @classmethod
    def _find_secrets(cls, *, pattern, secretsmanager_client):
        results = []
        for secret in cls._all_secrets(secretsmanager_client=secretsmanager_client):
            if re.search(pattern, secret['Name']):
                results.append(secret)
        return results

    @classmethod
    def _find_secret(cls, *, pattern, secretsmanager_client):
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
        secretsmanager_client = secretsmanager_client or boto3.client('secretsmanager')
        return cls._find_secret(secretsmanager_client=secretsmanager_client, pattern=pattern)

    @classmethod
    def find_application_secrets_table(cls, secretsmanager_client=None,
                                       application_configuration_pattern='ApplicationConfiguration'):
        secretsmanager_client = secretsmanager_client or boto3.client('secretsmanager')
        secret = cls.find_secret(pattern=application_configuration_pattern,
                                 secretsmanager_client=secretsmanager_client)
        secret_name = secret['Name']
        return SecretsTable(name=secret_name, secretsmanager_client=secretsmanager_client)
