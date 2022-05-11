import time
import urllib.parse

from botocore.exceptions import ClientError
from .common import APP_CGAP, APP_FOURFRONT
from .env_utils import (
    is_cgap_env, is_fourfront_env, get_standard_mirror_env,
    compute_orchestrated_prd_env_for_project, compute_orchestrated_real_url,
)
from .misc_utils import PRINT


FOURSIGHT_URL = 'https://foursight.4dnucleome.org/'

# FF_MAGIC_CNAME corresponds to data.4dnucleome.org
_FF_MAGIC_CNAME = 'fourfront-green.us-east-1.elasticbeanstalk.com'
# CGAP_MAGIC_CNAME corresponds to cgap.hms.harvard.edu
_CGAP_MAGIC_CNAME = 'fourfront-cgap.9wzadzju3p.us-east-1.elasticbeanstalk.com'

# FF_GOLDEN_DB is the database behind data.4dnucleome.org (and shared by staging.4dnucleome.org)
_FF_GOLDEN_DB = 'fourfront-production.co3gwj7b7tpq.us-east-1.rds.amazonaws.com'
# CGAP_GOLDEN_DB is the database behind cgap.hms.harvard.edu
_CGAP_GOLDEN_DB = 'fourfront-cgap.co3gwj7b7tpq.us-east-1.rds.amazonaws.com'


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
    url = get_beanstalk_real_url(env)
    parsed_url = urllib.parse.urlparse(url)

    return {
        'EnvironmentName': env,
        'CNAME': parsed_url.hostname,
    }


def get_beanstalk_real_url(env):
    """
    Return the real url for the elasticbeanstalk with given environment name.
    Name can be 'cgap', 'data', 'staging', or an actual environment.

    Args:
        env (str): ElasticBeanstalk environment name

    Returns:
        str: url of the ElasticBeanstalk environment
    """

    return compute_orchestrated_real_url(env)


def compute_ff_prd_env():  # a.k.a. "whodaman" (its historical name, which has gone away)
    """Returns the name of the current Fourfront production environment."""
    return compute_orchestrated_prd_env_for_project(APP_FOURFRONT)


def compute_ff_stg_env():
    """Returns the name of the current Fourfront staging environment."""
    return get_standard_mirror_env(compute_ff_prd_env())


def compute_cgap_prd_env():
    """Returns the name of the current CGAP production environment."""
    return compute_orchestrated_prd_env_for_project(APP_CGAP)


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


def compute_stg_env_for_env(envname):
    return get_standard_mirror_env(compute_prd_env_for_env(envname))
