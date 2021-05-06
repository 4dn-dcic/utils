import os
import boto3
import json
from botocore.exceptions import ClientError
from dcicutils.ecr_utils import CGAP_ECR_REGION


def assume_identity():
    """ Grabs application identity from the secrets manager.
        Looks for environment variable IDENTITY, which should contain the name of
        a secret in secretsmanager that is a JSON blob of core configuration information.
        Default value is current value in the test account. This name should be the
        name of the environment.
    """
    secret_name = os.environ.get('IDENTITY', 'dev/beanstalk/cgap-dev')
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
        if e.response['Error']['Code'] == 'DecryptionFailureException':
            # Secrets Manager can't decrypt the protected secret text using the provided KMS key.
            raise e
        elif e.response['Error']['Code'] == 'InternalServiceErrorException':
            # An error occurred on the server side.
            raise e
        elif e.response['Error']['Code'] == 'InvalidParameterException':
            # You provided an invalid value for a parameter.
            raise e
        elif e.response['Error']['Code'] == 'InvalidRequestException':
            # You provided a parameter value that is not valid for the current state of the resource.
            raise e
        elif e.response['Error']['Code'] == 'ResourceNotFoundException':
            raise e
        else:
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
