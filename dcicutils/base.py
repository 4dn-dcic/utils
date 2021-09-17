import boto3
import time

from botocore.exceptions import ClientError
from dcicutils.misc_utils import PRINT
from dcicutils.env_utils import (
    is_cgap_env, is_fourfront_env, is_stg_or_prd_env, public_url_mappings,
    blue_green_mirror_env, get_standard_mirror_env,
)


REGION = 'us-east-1'

FOURSIGHT_URL = 'https://foursight.4dnucleome.org/'

# FF_MAGIC_CNAME corresponds to data.4dnucleome.org
FF_MAGIC_CNAME = 'fourfront-green.us-east-1.elasticbeanstalk.com'
# CGAP_MAGIC_CNAME corresponds to cgap.hms.harvard.edu
CGAP_MAGIC_CNAME = 'fourfront-cgap.9wzadzju3p.us-east-1.elasticbeanstalk.com'

# FF_GOLDEN_DB is the database behind data.4dnucleome.org (and shared by staging.4dnucleome.org)
FF_GOLDEN_DB = 'fourfront-production.co3gwj7b7tpq.us-east-1.rds.amazonaws.com'
# CGAP_GOLDEN_DB is the database behind cgap.hms.harvard.edu
CGAP_GOLDEN_DB = 'fourfront-cgap.co3gwj7b7tpq.us-east-1.rds.amazonaws.com'


def describe_beanstalk_environments(client, **kwargs):
    """
    Generic function for retrying client.describe_environments to avoid
    AWS throttling errors. Passes all given kwargs to describe_environments

    Args:
        client (botocore.client.ElasticBeanstalk): boto3 client

    Returns:
        dict: response from client.describe_environments

    Raises:
        Exception: if a non-ClientError exception is encountered during
            describe_environments or cannot complete within retry framework
    """
    env_info = kwargs.get('EnvironmentNames', kwargs.get('ApplicationName', 'Unknown environment'))
    for retry in [1, 1, 1, 1, 2, 2, 2, 4, 4, 6, 8, 10, 12, 14, 16, 18, 20]:
        try:
            res = client.describe_environments(**kwargs)
        except ClientError as e:
            PRINT('Client exception encountered while getting BS info for %s. Error: %s' % (env_info, str(e)))
            time.sleep(retry)
        except Exception as e:
            PRINT('Unhandled exception encountered while getting BS info for %s. Error: %s' % (env_info, str(e)))
            raise e
        else:
            return res
    raise Exception('Could not describe Beanstalk environments due ClientErrors, likely throttled connections.')


def beanstalk_info(env):
    """
    Describe a ElasticBeanstalk environment given an environment name

    Args:
        env (str): ElasticBeanstalk environment name

    Returns:
        dict: Environments result from describe_beanstalk_environments
    """
    client = boto3.client('elasticbeanstalk', region_name=REGION)
    res = describe_beanstalk_environments(client, EnvironmentNames=[env])
    envs = res['Environments']
    if not envs:
        # Raise an error that will be meaningful to the caller, rather than just getting an index out of range error.
        raise ClientError({"Error": {"Code": 404, "Message": f"Environment does not exist: {env}"}},
                          # Properly speaking, this error does not come from .describe_environments(), so we kind of
                          # have to make up an operation that's failing, even though it's not a boto3 operation.
                          operation_name="beanstalk_info")
    else:
        return envs[0]


def get_beanstalk_real_url(env):
    """
    Return the real url for the elasticbeanstalk with given environment name.
    Name can be 'cgap', 'data', 'staging', or an actual environment.

    Args:
        env (str): ElasticBeanstalk environment name

    Returns:
        str: url of the ElasticBeanstalk environment
    """
    urls = public_url_mappings(env)

    if env in urls:  # Special case handling of 'cgap', 'data', or 'staging' as an argument.
        return urls[env]

    if is_stg_or_prd_env(env):
        # What counts as staging/prod depends on whether we're in the CGAP or Fourfront space.
        data_env = compute_cgap_prd_env() if is_cgap_env(env) else compute_ff_prd_env()
        # There is only one production environment. Everything else is staging, but everything
        # else is not staging.4dnucleome.org. Only one is that.
        if env == data_env:
            return urls['data']
        elif env == blue_green_mirror_env(data_env):
            # Mirror env might be None, in which case this clause will not be entered
            return urls['staging']

    bs_info = beanstalk_info(env)
    url = "http://" + bs_info['CNAME']
    return url


def _compute_prd_env_for_project(project):
    """
    Determines which ElasticBeanstalk environment is currently hosting
    data.4dnucleome.org. Requires IAM permissions for EB!

    Returns:
        str: EB environment name hosting data.4dnucleome
    """
    magic_cname = CGAP_MAGIC_CNAME if project == 'cgap' else FF_MAGIC_CNAME
    client = boto3.client('elasticbeanstalk', region_name=REGION)
    res = describe_beanstalk_environments(client, ApplicationName="4dn-web")
    for env in res['Environments']:
        if env.get('CNAME') == magic_cname:
            # we found data
            return env.get('EnvironmentName')


def compute_ff_prd_env():  # a.k.a. "whodaman" (its historical name, only defined in beanstalk_utils)
    """Returns the name of the current Fourfront production environment."""
    return _compute_prd_env_for_project('fourfront')


def compute_ff_stg_env():
    """Returns the name of the current Fourfront staging environment."""
    return get_standard_mirror_env(compute_ff_prd_env())


def compute_cgap_prd_env():
    """Returns the name of the current CGAP production environment."""
    return _compute_prd_env_for_project('cgap')


def compute_cgap_stg_env():
    """Returns the name of the current CGAP staging environment, or None if there is none."""
    return get_standard_mirror_env(compute_cgap_prd_env())


def compute_prd_env_for_env(envname):
    """Given an environment, returns the name of the prod environment for its owning project."""
    if is_cgap_env(envname):
        return compute_cgap_prd_env()
    elif is_fourfront_env(envname):
        return compute_ff_prd_env()
    else:
        raise ValueError("Unknown environment: %s" % envname)
