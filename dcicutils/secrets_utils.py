import os
import boto3
import json
from botocore.exceptions import ClientError
from .ecr_utils import CGAP_ECR_REGION
from .misc_utils import PRINT


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
