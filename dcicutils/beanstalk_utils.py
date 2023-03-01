"""
Low-level shared utilities related to C4 (the union of CGAP and 4DN/Fourfront) deployment and management.
This intends to be a relatively minimal set of things required to bootstrap other libraries, breaking some
circular dependencies that previously tangled things up.
"""

import subprocess
import logging
import boto3
import os
import json
import requests
import time
from datetime import datetime
from . import ff_utils
from botocore.exceptions import ClientError
from .base import (
    FOURSIGHT_URL,  # _FF_MAGIC_CNAME, _CGAP_MAGIC_CNAME, _FF_GOLDEN_DB, _CGAP_GOLDEN_DB,
    beanstalk_info, describe_beanstalk_environments, get_beanstalk_real_url,
    compute_ff_prd_env, compute_ff_stg_env, compute_cgap_prd_env, compute_cgap_stg_env, compute_prd_env_for_env,
)
from .common import REGION
from .env_utils import is_stg_or_prd_env, is_orchestrated
from .misc_utils import PRINT, exported, obsolete, remove_suffix, prompt_for_input


exported(
    # These used to be defined here, and there may be other files or even repos that import these,
    # so retain them even if they aren't otherwise used in this file. This is NOT a full list of all
    # things defined in this file, but only what's needed to keep these looking like unused imports.
    # -kmp 16-Sep-2021
    REGION, FOURSIGHT_URL,  # _FF_MAGIC_CNAME, _CGAP_MAGIC_CNAME, _FF_GOLDEN_DB, _CGAP_GOLDEN_DB,
    beanstalk_info, describe_beanstalk_environments, get_beanstalk_real_url,
    compute_ff_prd_env, compute_ff_stg_env, compute_cgap_prd_env, compute_cgap_stg_env, compute_prd_env_for_env,
)


logging.basicConfig()
logger = logging.getLogger('logger')
logger.setLevel(logging.INFO)

# In Python 2, the safe 'input' function was called 'raw_input'.  Also in Python 2, there was a function
# named 'input' that did eval(raw_input(...)).  Python 3 made an incompatible change, renaming 'raw_input'
# to 'input', and it no longer has a function that does an unsafe eval.  When we supported both Python 2 & 3,
# use a 'try' expression to sort things out and call the safe function 'use_input' to avoid confusion.
# But PyCharm found that 'try' expression confusing, so now that we are Python 3 only, we're phasing that
# out. For a time, we'll retain the transitional naming, though, along with an affirmative error check, so
# we don't open any security holes.
# TODO: We can remove this naming and check once we're we're only using Python 3.
# -kmp 27-Mar-2020

# The name whodaman was deprecated and has been removed as of dcicutils 3.0
# Please use compute_ff_prd_env instead.
#
# whodaman = compute_ff_prd_env  # This naming is obsolete but retained for compatibility.

# The legacy name MAGIC_CNAME was deprecated and is finally removed as of dcicutils 3.0.
# MAGIC_CNAME = _FF_MAGIC_CNAME

# The legacy name GOLDEN_DB was deprecated and is finally removed as of dcicutils 3.0.
# GOLDEN_DB = _FF_GOLDEN_DB

# identifier for locating environment variables in EB config
ENV_VARIABLE_NAMESPACE = 'aws:elasticbeanstalk:application:environment'


class WaitingForBoto3(Exception):
    pass


def delete_db(db_identifier, take_snapshot=True, allow_delete_prod=False):
    """
    Given db_identifier, delete an RDS instance. If take_snapshot is true,
    will create a final snapshot named "<db_identifier>-final-<yyyy-mm-dd>".

    Args:
        db_identifier (str): name of RDS instance
        take_snapshot (bool): If True, take a final snapshot before deleting
        allow_delete_prod (bool): Must be True to allow deletion of 'webprod' DB

    Returns:
        dict: boto3 response from delete_db_instance
    """
    return _delete_db(db_identifier=db_identifier,
                      take_snapshot=take_snapshot,
                      allow_delete_prod=allow_delete_prod)


def _delete_db(db_identifier, take_snapshot=True, allow_delete_prod=False):
    """Internal version of delete_db."""
    # safety. Do not allow accidental programmatic deletion of webprod DB
    if 'prod' in db_identifier and not allow_delete_prod:
        raise Exception('Must set allow_delete_prod to True to delete RDS instance' % db_identifier)
    client = boto3.client('rds')
    timestamp = datetime.strftime(datetime.utcnow(), "%Y-%m-%d")
    if take_snapshot:
        try:
            resp = client.delete_db_instance(
                DBInstanceIdentifier=db_identifier,
                SkipFinalSnapshot=False,
                FinalDBSnapshotIdentifier=db_identifier + "-final-" + timestamp
            )
        except:  # noqa: E722
            # Snapshot cannot be made. Likely a date conflict
            resp = client.delete_db_instance(
                DBInstanceIdentifier=db_identifier,
                SkipFinalSnapshot=True,
            )
    else:
        resp = client.delete_db_instance(
            DBInstanceIdentifier=db_identifier,
            SkipFinalSnapshot=True,
        )
    PRINT(resp)
    return resp


@obsolete
def get_es_from_bs_config(env):
    """
    Given an ElasticBeanstalk environment name, get the corresponding
    Elasticsearch url from the EB configurationock

    Args:
        env (str): ElasticBeanstalk environment name

    Returns:
        str: Elasticsearch url without port info
    """
    bs_env = get_bs_env(env)
    for item in bs_env:
        if item.startswith('ES_URL'):
            return item.split('=')[1].strip(':80')


@obsolete
def is_indexing_finished(env, prev_version=None, travis_build_id=None):
    """
    Checker function used with torb waitfor lambda; output must be standarized.
    Check to see if indexing of a Fourfront environment corresponding to given
    ElasticBeanstalk environment is finished by looking at the /counts page.

    Args:
        env (str): ElasticBeanstalk environment name
        prev_version (str): optional EB version of the previous configuration
        travis_build_id (int or str): optional ID for a Travis build

    Returns:
        bool, list: True if done, results from /counts page

    Raises:
        Exception: if Travis done and bad EB environment VersionLabel
        WaitingForBoto3: on a retryable waitfor condition
    """
    # is_beanstalk_ready will raise WaitingForBoto3 if not ready
    is_beanstalk_ready(env)
    bs_url = get_beanstalk_real_url(env)
    if not bs_url.endswith('/'):
        bs_url += "/"

    # retry if the beanstalk version has not updated from previous version,
    # unless the travis build has failed (in which case it will never update).
    # If failed, let is_indexing continue as usual so the deployment will
    # complete, despite being on the wrong EB application version
    if prev_version and beanstalk_info(env).get('VersionLabel') == prev_version:
        if travis_build_id:
            try:
                trav_done, trav_details = is_travis_finished(str(travis_build_id))
            except Exception as exc:
                # if the build failed, let the indexing check continue
                if 'Build Failed' in str(exc):
                    logger.info("EB version has not updated from %s."
                                "Associated travis build %s has failed."
                                % (prev_version, travis_build_id))
                else:
                    raise WaitingForBoto3("EB version has not updated from %s. "
                                          "Encountered error when getting build"
                                          " %s from Travis. Error: %s"
                                          % (prev_version, travis_build_id, exc))
            else:
                # Travis build is running/has passed
                if trav_done is True:
                    logger.info("EB version has not updated from %s."
                                "Associated travis build %s has finished."
                                % (prev_version, travis_build_id))
                else:
                    raise WaitingForBoto3("EB version has not updated from %s."
                                          "Associated travis build is %s and "
                                          "has not yet finished. Details: %s"
                                          % (prev_version, travis_build_id, trav_details))
        else:
            # no build ID provided; must retry on not updated version
            raise WaitingForBoto3("EB version has not updated from %s"
                                  % prev_version)

    # check counts from the portal to determine indexing state
    try:
        counts_res = ff_utils.authorized_request(bs_url + 'counts?format=json',
                                                 ff_env=env)
        totals = counts_res.json().get('db_es_total').split()

        # example value of split totals: ["DB:", "74048", "ES:", "74048"]
        db_total = totals[1]
        es_total = totals[3]

        if int(db_total) > int(es_total):
            is_ready = False
        else:
            is_ready = True
    except Exception as exc:
        logger.info('Error on is_indexing_finished: %s' % exc)
        is_ready = False
        totals = []
    return is_ready, totals


def _swap_cname(src, dest):
    """
    Swap the CNAMEs of two ElasticBeanstalk (EB) environments, given by
    src and dest. Will restart the app servers after swapping.
    Should be used for swapping production/staging environments.

    Args:
        src (str): EB environment name of production
        dest (str): EB environment name of staging

    Returns:
        None
    """
    client = boto3.client('elasticbeanstalk', region_name=REGION)
    PRINT("Swapping CNAMEs %s and %s..." % (src, dest))
    client.swap_environment_cnames(SourceEnvironmentName=src,
                                   DestinationEnvironmentName=dest)
    PRINT("Giving CNAMEs 10 seconds to update...")
    time.sleep(10)
    PRINT("Restarting app servers for %s and %s..." % (src, dest))
    client.restart_app_server(EnvironmentName=src)
    client.restart_app_server(EnvironmentName=dest)


def _create_foursight_new(dest_env):
    """ Helper function that does what create_foursight_auto used to do but slightly differently """
    fs = {  # noQA - PyCharm thinks the way this dictionary is set up could be simplified, but it's fine for now.
        'dest_env': dest_env,
        'bs_url': get_beanstalk_real_url(dest_env)
    }

    # Get information, pass to create_foursight
    fs['fs_url'] = get_foursight_env(dest_env, fs['bs_url'])
    fs['es_url'] = ff_utils.get_health_page(ff_env=dest_env)['elasticsearch']
    fs['foursight'] = create_foursight(**fs)
    if is_orchestrated():
        # TODO: Probably want to inherit some values from the old file in this case, since not all of those change.
        raise NotImplementedError("Need to add orchestration support here.")
    else:
        fs['is_legacy'] = True

    # delete initial checks (? not clear why this was happening before)
    if fs['foursight'].get('initial_checks'):
        del fs['foursight']['initial_checks']

    return fs


# This function has been removed on a major version boundary. This is no longer the way to swap staging
# and data identies.
#
# def swap_cname(src, dest):
#     """ Does a CNAME swap and foursight configuration (pulled in from Torb)
#         NOTE: this is the mechanism by which CNAME swaps must occur as of 9/15/2020
#     """
#     _swap_cname(src, dest)
#     res_data = _create_foursight_new(src)
#     print('Updated foursight %s environment to use %s. Foursight response: %s'
#           % (res_data['fs_url'], res_data['dest_env'], res_data['foursight']))
#     res_stag = _create_foursight_new(dest)
#     print('Updated foursight %s environment to use %s. Foursight response: %s'
#           % (res_stag['fs_url'], res_stag['dest_env'], res_stag['foursight']))


def _get_beanstalk_configuration_settings(env):
    """ Helper function for the below method (that is easy to mock for testing).
        This function should not be called directly.
        (relevant) syntax from boto3 docs:
            {
                'ConfigurationSettings': [
                    {
                        'SolutionStackName': 'string',
                        'PlatformArn': 'string',
                        'ApplicationName': 'string',
                        'TemplateName': 'string',
                        'Description': 'string',
                        'EnvironmentName': 'string',
                        'DeploymentStatus': 'deployed'|'pending'|'failed',
                        'DateCreated': datetime(2015, 1, 1),
                        'DateUpdated': datetime(2015, 1, 1),
                        'OptionSettings': [
                            {
                                'ResourceName': 'string',
                                'Namespace': 'string',
                                'OptionName': 'string',
                                'Value': 'string'
                            },
                        ]
                    },
                ]
            }
    """
    try:
        client = boto3.client('elasticbeanstalk', region_name=REGION)
        config = client.describe_configuration_settings(ApplicationName='4dn-web', EnvironmentName=env)
        # These are guaranteed to be present
        [settings] = config['ConfigurationSettings']
        options = settings['OptionSettings']
        return options
    except ClientError:
        logger.error('Error encountered attempting to get environment settings for %s' % env)
        return []


def get_beanstalk_environment_variables(env):
    """ Acquires the environment variables used to deploy the given environment.

        VERY IMPORTANT NOTE: this function will echo *extremely sensitive* data if run.
        Ensure that if you are using this you are not logging the output of this anywhere.
    """
    options = _get_beanstalk_configuration_settings(env)
    env = {}
    for option in options:
        if 'Namespace' in option:
            if option['Namespace'] == ENV_VARIABLE_NAMESPACE:
                env[option['OptionName']] = option['Value']
    return env


@obsolete
def is_beanstalk_ready(env):
    """
    Checker function used with torb waitfor lambda; output must be standarized.
    Check to see if a ElasticBeanstalk environment status is "Ready"

    Args:
        env (str): ElasticBeanstalk environment name

    Returns:
        bool, str: True if done, ElasticBeanstalk url

    Raises:
        WaitingForBoto3: if EB environment status != "Ready"
    """
    client = boto3.client('elasticbeanstalk', region_name=REGION)
    res = describe_beanstalk_environments(client, EnvironmentNames=[env])

    status = res['Environments'][0]['Status']
    if status != 'Ready':
        raise WaitingForBoto3("Beanstalk environment status is %s" % status)

    return True, 'http://' + res['Environments'][0].get('CNAME')


def is_snapshot_ready(snapshot_name):
    """
    Checker function used with torb waitfor lambda; output must be standarized.
    Check to see if an RDS snapshot with given name is available

    Args:
        snapshot_name (str): RDS snapshot name

    Returns:
        bool, str: True if done, identifier of snapshot
    """
    client = boto3.client('rds', region_name=REGION)
    resp = client.describe_db_snapshots(DBSnapshotIdentifier=snapshot_name)
    db_status = resp['DBSnapshots'][0]['Status']
    is_ready = db_status.lower() == 'available'
    return is_ready, resp['DBSnapshots'][0]['DBSnapshotIdentifier']


@obsolete
def is_es_ready(es_name):
    """
    Checker function used with torb waitfor lambda; output must be standarized.
    Check to see if an ES instance is ready and has an endpoint

    Args:
        es_name (str): ES instance name

    Returns:
        bool, str: True if done, ES url
    """
    es = boto3.client('es', region_name=REGION)
    describe_resp = es.describe_elasticsearch_domain(DomainName=es_name)
    endpoint = describe_resp['DomainStatus'].get('Endpoint', None)
    is_ready = True
    if endpoint is None:
        is_ready = False
    else:
        endpoint = endpoint + ":80"
    return is_ready, endpoint


def is_db_ready(db_identifier):
    """
    Checker function used with torb waitfor lambda; output must be standarized.
    Check to see if an RDS instance with given name is ready

    Args:
        db_identifier (str): RDS instance identifier

    Returns:
        bool, str: True if done, RDS address
    """
    client = boto3.client('rds', region_name=REGION)
    is_ready = False
    resp = client.describe_db_instances(DBInstanceIdentifier=db_identifier)
    details = resp
    endpoint = resp['DBInstances'][0].get('Endpoint')
    if endpoint and endpoint.get('Address'):
        details = endpoint['Address']
        is_ready = True

    return is_ready, details


def create_db_snapshot(db_identifier, snapshot_name):
    """
    Given an RDS instance indentifier, create a snapshot using the given name.
    If a snapshot with given name already exists, attempt to delete and return
    "Deleting". Otherwise, return snapshot ARN.

    Args:
        db_identifier (str): RDS instance identifier
        snapshot_name (str): identifier/ARN of RDS snapshot to create

    Returns:
        str: resource ARN if successful, otherwise "Deleting"
    """
    client = boto3.client('rds', region_name=REGION)
    try:
        response = client.create_db_snapshot(
             DBSnapshotIdentifier=snapshot_name,
             DBInstanceIdentifier=db_identifier)
    except ClientError:
        # probably the guy already exists
        try:
            client.delete_db_snapshot(DBSnapshotIdentifier=snapshot_name)
        except ClientError:
            pass
        return "Deleting"

    return response


def create_db_from_snapshot(db_identifier, snapshot_name, delete_db_if_present=True):
    """
    Given an RDS instance indentifier and a snapshot ARN/name, create an RDS
    instance from the snapshot. If an instance already exists with the given
    identifier and delete_db is True, attempt to delete and return "Deleting".
    Otherwise, return instance ARN.

    Args:
        db_identifier (str): RDS instance identifier
        snapshot_name (str): identifier/ARN of RDS snapshot to restore from
        delete_db_if_present(bool): whether to drop the database on unwind

    Returns:
        str: resource ARN if successful, otherwise "Deleting"
    """
    client = boto3.client('rds', region_name=REGION)
    try:
        response = client.restore_db_instance_from_db_snapshot(
            DBInstanceIdentifier=db_identifier,
            DBSnapshotIdentifier=snapshot_name,
            DBInstanceClass='db.t2.medium',
            StorageType='gp2',
            PubliclyAccessible=False
        )
    except ClientError:
        # Something went wrong
        # Even if delete_db, never allow deletion of a db with 'production' in it
        if delete_db_if_present and 'production' not in db_identifier:
            # Drop target database with final snapshot
            try:
                _delete_db(db_identifier, take_snapshot=True)
            except ClientError:
                pass
            return "Deleting"
        else:
            return "Error"
    return response['DBInstance']['DBInstanceArn']


@obsolete
def is_travis_started(request_url):
    """
    Checker function used with torb waitfor lambda; output must be standarized.
    Check the requests url to see if a given build has stared and been issued
    a build id, which can in turn be used for is_travis_finished

    Args:
        request_url (str): Travis request url

    Returns:
        bool, dict: True if started, Travis response JSON

    Raises:
        Exception: if Travis key not in environ
    """
    if 'travis_key' not in os.environ:
        raise Exception('Must have travis_key environment variable defined')
    is_ready = False
    details = 'requested build has not started'
    headers = {'Content-Type': 'application/json',
               'Accept': 'application/json',
               'Travis-API-Version': '3',
               'User-Agent': 'tibanna/0.1.0',
               'Authorization': 'token %s' % os.environ['travis_key']}
    resp = requests.get(request_url, headers=headers)
    if resp.ok:
        logger.info("Travis request response (okay): %s" % resp.json())
        details = resp.json()
        if len(resp.json().get('builds', [])) == 1:
            is_ready = True
    return is_ready, details


@obsolete
def is_travis_finished(build_id):
    """
    Checker function used with torb waitfor lambda; output must be standarized.
    Check to see if a given travis build has passed

    Args:
        build_id (int or str): Travis build identifier

    Returns:
        bool, dict: True if done, Travis response JSON

    Raises:
        Exception: if the Travis build failed or Travis key not in environ
    """
    if 'travis_key' not in os.environ:
        raise Exception('Must have travis_key environment variable defined')
    is_ready = False
    details = 'build not done or not found'
    headers = {'Content-Type': 'application/json',
               'Accept': 'application/json',
               'Travis-API-Version': '3',
               'User-Agent': 'tibanna/0.1.0',
               'Authorization': 'token %s' % os.environ['travis_key']}

    url = 'https://api.travis-ci.org/build/%s' % build_id

    logger.info("Travis build url: %s" % url)
    resp = requests.get(url, headers=headers)
    logger.info("Travis build response: %s" % resp.text)
    state = resp.json()['state']
    if resp.ok and state in ['failed', 'errored']:
        raise Exception('Build Failed')
    elif resp.ok and state == 'passed':
        is_ready = True
        details = resp.json()
    return is_ready, details


def make_envvar_option(name, value):
    return {
        'Namespace': 'aws:elasticbeanstalk:application:environment',
        'OptionName': name,
        'Value': value
    }


def get_bs_env(envname):
    """
    Given an ElasticBeanstalk environment name, get the env variables from that
    environment and return them. Returned variables are in form: <name>=<value>

    Args:
        envname (str): name of ElasticBeanstalk environment

    Returns:
        list: of environment variables in <name>=<value> form
    """
    client = boto3.client('elasticbeanstalk', region_name=REGION)

    data = client.describe_configuration_settings(EnvironmentName=envname,
                                                  ApplicationName='4dn-web')
    options = data['ConfigurationSettings'][0]['OptionSettings']
    env_vars = [option['Value'] for option in options
                if option['OptionName'] == 'EnvironmentVariables'][0]
    return env_vars.split(',')


@obsolete
def update_bs_config(envname, template=None, keep_env_vars=False,
                     env_override=None):
    """
    Update the configuration for an existing ElasticBeanstalk environment.
    Requires the environment name. Can optionally specify a configuration
    template, as well as keep all environment variables from the existing
    environment with optional variable overrides.

    Args:
        envname (str): name of the EB environment
        template (str): configuration template to use. Default None
        keep_env_vars (bool): if True, keep existing env vars. Default False
        env_override (dict): if provided, overwrite existing env vars using the
            given key/values. Must use keep_env_vars to work. Default None

    Returns:
        dict: update_environment response
    """
    if template is None and not keep_env_vars:
        # nothing to update
        logger.info("update_bs_config: nothing to update for env %s!" % envname)
        return None
    client = boto3.client('elasticbeanstalk', region_name=REGION)
    options = []  # used to hold env vars
    if keep_env_vars:
        env_vars = get_bs_env(envname)
        for var in env_vars:
            key, value = var.split('=')
            if env_override and env_override.get(key):
                options.append(make_envvar_option(key, env_override[key]))
            else:
                options.append(make_envvar_option(key, value))

    # update template and/or env var options
    if options and template:
        return client.update_environment(EnvironmentName=envname,
                                         TemplateName=template,
                                         OptionSettings=options)
    elif template:
        return client.update_environment(EnvironmentName=envname,
                                         TemplateName=template)
    else:
        return client.update_environment(EnvironmentName=envname,
                                         OptionSettings=options)


@obsolete
def create_bs(envname, load_prod, db_endpoint, es_url, for_indexing=False):
    """
    XXX: Will not work currently, do NOT use on production

    Create a beanstalk environment given an envname. Use customized options,
    configuration template, and environment variables. If adding new env vars,
    make sure to overwrite them here.
    If the environment already exists, will update it with `update_bs_config`

    Args:
        envname (str): ElasticBeanstalk (EB) enviroment name
        load_prod (bool): sets the LOAD_FUNCTION EB env var
        db_endpoint (str): sets the RDS_HOSTNAME EB env var
        es_url (str): sets the ES_URL EB env var
        for_indexing (bool): If True, use 'fourfront-indexing' config template

    Returns:
        dict: boto3 res from create_environment/update_environment
    """
    # TODO (C4-280): Reconsider this and other functionality.
    if is_stg_or_prd_env(envname):
        raise RuntimeError("beanstalk_utils.create_bs is not approved for production use.")

    client = boto3.client('elasticbeanstalk', region_name=REGION)

    # determine the configuration template for Elasticbeanstalk
    template = 'fourfront-base'
    if for_indexing:
        template = 'fourfront-indexing'
    load_value = 'load_test_data'
    if load_prod:
        load_value = 'load_prod_data'

    options = [
        make_envvar_option('RDS_HOSTNAME', db_endpoint),
        make_envvar_option('ENV_NAME', envname),
        make_envvar_option('ES_URL', es_url),
        make_envvar_option('LOAD_FUNCTION', load_value)
    ]

    # logic for mirrorEsEnv, which is used to coordinate elasticsearch
    # changes between fourfront data and staging
    if 'fourfront-webprod' in envname:
        # TODO: This code is obsolete and needs to be upgraded. For now, the use of this on production is disabled.
        other_env = 'fourfront-webprod2' if envname == 'fourfront-webprod' else 'fourfront-webprod'
        mirror_es = get_es_build_status(other_env, max_tries=3)
        if mirror_es:
            options.append(make_envvar_option('mirrorEnvEs', mirror_es))

    try:
        res = client.create_environment(
            ApplicationName='4dn-web',
            EnvironmentName=envname,
            TemplateName=template,
            OptionSettings=options,
        )
    except ClientError:
        # environment already exists update it with given template
        # parse out current env variables and override existing values
        env_vars = {opt['OptionName']: opt['Value'] for opt in options}
        res = update_bs_config(envname, template=template, keep_env_vars=True,
                               env_override=env_vars)
    return res


# location of environment variables on elasticbeanstalk
BEANSTALK_ENV_PATH = "/opt/python/current/env"


def source_beanstalk_env_vars(config_file=BEANSTALK_ENV_PATH):
    """
    set environment variables if we are on Elastic Beanstalk
    AWS_ACCESS_KEY_ID is indicative of whether or not env vars are sourced

    Args:
        config_file (str): filepath to load env vars from
    """
    if os.path.exists(config_file) and not os.environ.get("AWS_ACCESS_KEY_ID"):
        command = ['bash', '-c', 'source ' + config_file + ' && env']
        proc = subprocess.Popen(command, stdout=subprocess.PIPE, universal_newlines=True)
        for line in proc.stdout:
            key, _, value = line.partition("=")
            os.environ[key] = value[:-1]
        proc.communicate()


def log_to_foursight(event, lambda_name='', overrides=None):
    """
    Use Foursight as a logging tool within in a lambda function by doing a PUT
    to /api/checks. Requires that the event has "_foursight" key, which is a
    subobject with the following:
    fields:
        "check": required, in form "<fs environment>/<check name>"
        "log_desc": will set "summary" and "description" if those are missing
        "full_output": optional. If not provided, use to provide info on lambda
        "brief_output": optional
        "summary": optional. If not provided, use "log_desc" value
        "description": optional. If not provided, use "log_desc" value
        "status": optional. If not provided, use "WARN"
    Can also optionally provide an dictionary to overrides param, which will
    update the event["_foursight"]

    Args:
        event (dict): Event input, most likely from a lambda with a workflow
        lambda_name (str): Name of the lambda that is calling this
        overrides (dict): Optionally override event['_foursight'] with this

    Returns:
        Response object from foursight

    Raises:
        Exception: if cannot get body from Foursight response
    """
    fs = event.get('_foursight')
    if fs and fs.get('check'):
        if overrides is not None and isinstance(overrides, dict):
            fs.update(overrides)
        # handles these fields. set full_output as a special case
        full_output = fs.get('full_output', '%s started to run' % lambda_name)
        brief_output = fs.get('brief_output')
        summary = fs.get('summary', fs.get('log_desc'))
        description = fs.get('description', fs.get('log_desc'))
        status = fs.get('status', 'WARN')

        data = {'status': status, 'summary': summary, 'description': description,
                'full_output': full_output, 'brief_output': brief_output}
        fs_auth = os.environ.get('FS_AUTH')
        headers = {'content-type': "application/json", 'Authorization': fs_auth}
        # fs['check'] should be in form: "<fs environment>/<check name>"
        url = FOURSIGHT_URL + 'checks/' + fs['check']
        res = requests.put(url, data=json.dumps(data), headers=headers)
        PRINT('Foursight response from %s: %s' % (url, res.text))
        try:
            return res.json()
        except Exception as exc:
            raise Exception('Error putting FS check to %s with body %s. '
                            'Exception: %s. Response text: %s'
                            % (url, data, exc, res.text))


@obsolete
def create_foursight_auto(dest_env):
    """
    Call `create_foursight` to create a Foursight environment based off a
    just a ElasticBeanstalk environment name. Determines a number of fields
    needed for the environment creation automatically. Also causes initial
    checks to be run on the new FS environment.

    Args:
        dest_env (str): ElasticBeanstalk environment name

    Returns:
        dict: response from Foursight PUT /api/environments
    """
    fs = {  # noQA - PyCharm thinks the way this dictionary is set up could be simplified, but it's fine for now.
        'dest_env': dest_env
    }

    # automatically determine info for FS environ creation
    fs['bs_url'] = get_beanstalk_real_url(dest_env)
    fs['fs_url'] = get_foursight_env(dest_env, fs['bs_url'])
    fs['es_url'] = get_es_from_bs_config(dest_env)

    fs['foursight'] = create_foursight(**fs)
    if fs['foursight'].get('initial_checks'):
        del fs['foursight']['initial_checks']

    return fs


def get_foursight_env(dest_env, bs_url=None):
    """
    Get a Foursight environment name corresponding the given ElasticBeanstalk
    environment name, with optionally providing the EB url for must robustness

    Args:
        dest_env (str): ElasticBeanstalk environment name
        bs_url (str): optional url for the ElasticBeanstalk instance

    Returns:
        str: Foursight environment name
    """
    if not bs_url:
        bs_url = get_beanstalk_real_url(dest_env)
    env = dest_env
    if 'data.4dnucleome.org' in bs_url:
        env = 'data'
    elif 'staging.4dnucleome.org' in bs_url:
        env = 'staging'
    return env


def create_foursight(dest_env, bs_url, es_url, fs_url=None):
    """
    Creates a new Foursight environment based off of dest_env. Since Foursight
    environments don't include "fourfront-" by convention, remove this if part
    of the env. Take some other options for settings on the env

    Note: this will cause all checks in all schedules to be run, to initialize
          environment.

    Args:
        dest_env (str): ElasticBeanstalk environment name
        bs_url (str): url of the ElasticBeanstalk for FS env
        es_url (str): url of the ElasticSearch for FS env
        fs_url (str): If provided, use to override dest-env based FS url

    Returns:
        dict: response from Foursight PUT to /api/environments

    Raises:
        Exception: if cannot get body from Foursight response
    """
    # we want some url like thing
    if not bs_url.startswith('http'):
        bs_url = 'http://' + bs_url
    if not bs_url.endswith('/'):
        bs_url += '/'

    if ':80' in es_url:
        es_url = remove_suffix(':80', es_url)
    elif ':443' in es_url:
        es_url = remove_suffix(':443', es_url)

    if not es_url.startswith('http'):
        es_url = 'https://' + es_url
    if not es_url.endswith('/'):
        es_url += '/'

    # environments on foursight don't include fourfront
    if not fs_url:
        fs_url = dest_env
    if fs_url.startswith('fourfront-'):
        fs_url = fs_url[len('fourfront-'):]

    foursight_url = FOURSIGHT_URL + 'environments/' + fs_url
    payload = {'fourfront': bs_url, 'es': es_url, 'ff_env': dest_env}

    ff_auth = os.environ.get('FS_AUTH')
    headers = {'content-type': 'application/json', 'Authorization': ff_auth}
    res = requests.put(foursight_url, data=json.dumps(payload), headers=headers)
    try:
        return res.json()
    except Exception as exc:
        raise Exception('Error creating FS environ to %s with body %s. '
                        'Exception: %s. Response text: %s'
                        % (foursight_url, payload, exc, res.text))


@obsolete
def create_new_es(new):
    """
    Create a new Elasticsearch domain with given name. See the
    args below for the settings used.

    TODO: do we want to add cognito and access policy setup here?
          I think not, since that's a lot of info to put out there...

    Args:
        new (str): Elasticsearch domain name

    Returns:
        dict: response from boto3 client
    """
    es = boto3.client('es', region_name=REGION)
    resp = es.create_elasticsearch_domain(
        DomainName=new,
        ElasticsearchVersion='5.3',
        ElasticsearchClusterConfig={
            'InstanceType': 'm4.large.elasticsearch',
            'InstanceCount': 2
        },
        EBSOptions={
            "EBSEnabled": True,
            "VolumeType": "gp2",
            "VolumeSize": 50
        }
    )
    PRINT('=== CREATED NEW ES DOMAIN %s ===' % new)
    PRINT('NO MASTER INSTANCES ARE USED!')
    PRINT('MAKE SURE TO UPDATE COGNITO AND ACCESS POLICY USING THE GUI!')
    PRINT(resp)

    return resp


@obsolete
def get_es_build_status(new, max_tries=None):
    """
    Check the build status of an Elasticsearch instance with given name.
    If max_tries is provided, only allow that many iterative checks to ES.
    Returns the ES endpoint plus port (80)

    Args:
        new (str): ES instance name
        max_tries (int): max number of times to check. Default None (no limit)

    Returns:
        str: ES endpoint plus port, or None if not found in max_tries
    """
    es = boto3.client('es', region_name=REGION)
    endpoint = None
    tries = 0
    while endpoint is None:
        describe_resp = es.describe_elasticsearch_domain(DomainName=new)
        endpoint = describe_resp['DomainStatus'].get('Endpoint')
        if max_tries is not None and tries >= max_tries:
            break
        if endpoint is None:
            PRINT(".")
            tries += 1
            time.sleep(10)

    # aws uses port 80 for es connection, lets be specific
    if endpoint:
        endpoint += ":80"
    PRINT('Found ES endpoint for %s: %s' % (new, endpoint))
    return endpoint


#########################################################################
# Functions meant to be used locally to clone or remove a beanstalk ENV #
# A lot of these functions use command line tools.                      #
# Sort of janky, but could end up being helpful someday ...             #
#########################################################################


@obsolete
def add_es(new, force_new=False):
    """
    Either gets information on an existing Elasticsearch instance
    or, if force_new is True, will create a new instance.

    Args:
        new (str): Fourfront EB environment name used for ES instance
        force_new (bool): if True, make a new ES. Default False

    Returns:
        str: AWS ARN of the ES instance
    """
    es = boto3.client('es', region_name=REGION)
    if force_new:
        # fallback is a new ES env to use if cannot create one with
        # `new` environment name
        fallback = new
        if new.endswith("-a"):
            fallback = fallback.replace("-a", "-b")
        elif new.endswith("-b"):
            fallback = fallback.replace("-b", "-a")
        else:
            fallback += "-a"
        try:
            resp = create_new_es(new)
        except:  # noqa: E722
            resp = create_new_es(fallback)
    else:
        try:
            resp = es.describe_elasticsearch_domain(DomainName=new)
        except ClientError:  # its not there
            resp = create_new_es(new)
    return resp['DomainStatus']['ARN']


def delete_es_domain(env_name):
    """
    Given an Elasticsearch domain name, delete the domain

    Args:
        env_name (str): Fourfront EB environment name used for ES instance

    Returns:
        None
    """
    # get the status of this bad boy
    es = boto3.client('es')
    try:
        res = es.delete_elasticsearch_domain(DomainName=env_name)
        PRINT(res)
    except:  # noqa: E722
        PRINT("es domain %s not found, skipping" % env_name)


@obsolete
def clone_bs_env_cli(old, new, load_prod, db_endpoint, es_url):
    """
    Use the eb command line client to clone an ElasticBeanstalk environment
    with some extra options.

    Args:
        old (str): existing EB environment name
        new (str): new EB environment name
        load_prod (bool): determins LOAD_FUNCTION EB env var
        db_endpoint (str): determines RDS_HOSTNAME EB env var
        es_url (str): determines ES_URL EB env var

    Returns:
        None
    """
    env = 'RDS_HOSTNAME=%s,ENV_NAME=%s,ES_URL=%s' % (db_endpoint, new, es_url)
    if load_prod is True:
        env += ",LOAD_FUNCTION=load_prod_data"
    subprocess.check_call(['eb', 'clone', old, '-n', new,
                           '--envvars', env,
                           '--exact', '--nohang'])


def delete_bs_env_cli(env_name):
    """
    Use the eb command line client to remove an ElasticBeanstalk environment
    with some extra options.

    Args:
        env_name (str): EB environment name

    Returns:
        None
    """
    subprocess.check_call(['eb', 'terminate', env_name, '-nh', '--force'])


def create_s3_buckets(new):
    """
    Given an ElasticBeanstalk env name, create the following s3 buckets that
    are standard for any of our EB environments.

    Args:
        new (str): EB environment name

    Returns:
        None
    """
    new_buckets = [
        'elasticbeanstalk-%s-blobs' % new,
        'elasticbeanstalk-%s-files' % new,
        'elasticbeanstalk-%s-wfoutput' % new,
        'elasticbeanstalk-%s-system' % new,
    ]
    s3 = boto3.client('s3', region_name=REGION)
    for bucket in new_buckets:
        s3.create_bucket(Bucket=bucket)
    PRINT('=== CREATED NEW S3 BUCKETS ===' % new)
    PRINT('MAKE SURE TO UPDATE CORS POLICY FOR -files AND -wfoutput BUCKETS!')


def delete_s3_buckets(env_name):
    """
    Given an ElasticBeanstalk env name, remove the following s3 buckets that
    are standard for any of our EB environments.

    Args:
        env_name (str): EB environment name

    Returns:
        None
    """
    buckets = [
        'elasticbeanstalk-%s-blobs' % env_name,
        'elasticbeanstalk-%s-files' % env_name,
        'elasticbeanstalk-%s-wfoutput' % env_name,
        'elasticbeanstalk-%s-system' % env_name,
        'elasticbeanstalk-%s-metadata-bundles' % env_name,
        # note that tibanna logs are shared so are not so easy to delete
    ]

    s3 = boto3.resource('s3')
    for bucket in buckets:
        PRINT("deleting content for " + bucket)
        try:
            s3.Bucket(bucket).objects.delete()
            s3.Bucket(bucket).delete()
        except Exception:  # noqa: E722
            PRINT(bucket + " not found skipping...")


def snapshot_and_clone_db(db_identifier, snapshot_name):
    """
    Given a RDS instance identifier and snapshot name, will create a snapshot
    with that name and then spin up a new RDS instance named after the snapshot

    Args:
        db_identifier (str): original RDS identifier of DB to snapshot
        snapshot_name (str): identifier of snapshot AND new instance

    Returns:
        str: address of the new instance
    """
    client = boto3.client('rds', region_name=REGION)
    snap_res = create_db_snapshot(db_identifier, snapshot_name)
    if snap_res == 'Deleting':
        snap_res = client.create_db_snapshot(
            DBSnapshotIdentifier=snapshot_name,
            DBInstanceIdentifier=db_identifier
        )
    PRINT("Response from create db snapshot: %s" % snap_res)
    PRINT("Waiting for snapshot to create...")
    waiter = client.get_waiter('db_snapshot_completed')
    waiter.wait(DBSnapshotIdentifier=snapshot_name)
    PRINT("Done waiting, creating a new database with name %s" % snapshot_name)
    db_res = create_db_from_snapshot(snapshot_name, snapshot_name, False)
    if db_res == 'Error':
        raise Exception('Could not create DB %s; already exists' % snapshot_name)
    PRINT("Waiting for DB to be created from snapshot...")
    endpoint = ''
    while not endpoint:
        resp = client.describe_db_instances(DBInstanceIdentifier=snapshot_name)
        endpoint = resp['DBInstances'][0].get('Endpoint')
        if endpoint and endpoint.get('Address'):
            PRINT("We got an endpoint:", endpoint['Address'])
            return endpoint['Address']
        PRINT(".")
        time.sleep(10)


def add_to_auth0_client(new):
    """
    Given an ElasticBeanstalk env name, find the url and use it to update the
    callback URLs for Auth0

    Args:
        new (str): EB environment name

    Returns:
        None
    """
    client = boto3.client('elasticbeanstalk', region_name=REGION)
    url = None
    PRINT("Getting beanstalk URL for env %s..." % new)
    while url is None:
        env = describe_beanstalk_environments(client, EnvironmentNames=[new])
        url = env['Environments'][0].get('CNAME')
        if url is None:
            PRINT(".")
            time.sleep(10)
    auth0_client_update(url)


def remove_from_auth0_client(env_name):
    """
    Given an ElasticBeanstalk env name, find the url and remove it from the
    callback urls for Auth0

    Args:
        env_name (str): EB environment name

    Returns:
        None
    """
    client = boto3.client('elasticbeanstalk', region_name=REGION)
    url = None
    PRINT("Getting beanstalk URL for env %s..." % env_name)
    while url is None:
        env = client.describe_environments(EnvironmentNames=[env_name])
        url = env['Environments'][0].get('CNAME')
        if url is None:
            PRINT(".")
            time.sleep(10)
    auth0_client_remove(url)


def auth0_client_update(url):
    """
    Get a JWT for programmatic access to Auth0 using Client/Secret env vars.
    Then add the given `url` to the Auth0 callbacks list.

    Args:
        url (str): url to add to callbacks

    Returns:
        None
    """
    # generate a jwt to validate future requests
    client = os.environ['Auth0Client']
    secret = os.environ['Auth0Secret']
    payload = {'grant_type': 'client_credentials',
               'client_id': client,
               'client_secret': secret,
               'audience': 'https://hms-dbmi.auth0.com/api/v2/'}
    headers = {'content-type': 'application/json'}
    res = requests.post('https://hms-dbmi.auth0.com/oauth/token',
                        data=json.dumps(payload),
                        headers=headers)
    jwt = res.json()['access_token']

    client_url = 'https://hms-dbmi.auth0.com/api/v2/clients/%s' % client
    headers['authorization'] = 'Bearer %s' % jwt

    get_res = requests.get(client_url + '?fields=callbacks', headers=headers)
    callbacks = get_res.json()['callbacks']
    callbacks.append('http://' + url)
    client_data = {'callbacks': callbacks}

    update_res = requests.patch(client_url, data=json.dumps(client_data), headers=headers)
    PRINT('auth0 callback urls are: %s' % update_res.json().get('callbacks'))


def auth0_client_remove(url):
    """
    Get a JWT for programmatic access to Auth0 using Client/Secret env vars.
    Then use that to remove the given `url` from the Auth0 callbacks list.

    Args:
        url (str): url to remove from callbacks

    Returns:
        None
    """
    # generate a jwt to validate future requests
    client = os.environ['Auth0Client']
    secret = os.environ['Auth0Secret']
    payload = {'grant_type': 'client_credentials',
               'client_id': client,
               'client_secret': secret,
               'audience': 'https://hms-dbmi.auth0.com/api/v2/'}
    headers = {'content-type': 'application/json'}
    res = requests.post('https://hms-dbmi.auth0.com/oauth/token',
                        data=json.dumps(payload),
                        headers=headers)
    jwt = res.json()['access_token']
    client_url = 'https://hms-dbmi.auth0.com/api/v2/clients/%s' % client
    headers['authorization'] = 'Bearer %s' % jwt

    get_res = requests.get(client_url + '?fields=callbacks', headers=headers)
    callbacks = get_res.json()['callbacks']
    full_url = 'http://' + url
    try:
        idx = callbacks.index(full_url)
    except ValueError:
        PRINT(full_url + ' Not in auth0 auth, doesn\'t need to be removed')
        return
    if idx:
        callbacks.pop(idx)
    client_data = {'callbacks': callbacks}

    update_res = requests.patch(client_url, data=json.dumps(client_data), headers=headers)
    PRINT('auth0 callback urls are: %s' % update_res.json().get('callbacks'))


def copy_s3_buckets(new, old):
    """
    Given a new ElasticBeanstalk environment name and existing "old" one,
    create the given buckets and copy contents from the corresponding
    existing ones

    Args:
        new (str): new EB environment name
        old (str): existing EB environment name

    Returns:
        None
    """
    # each env needs the following buckets
    new_buckets = [
        'elasticbeanstalk-%s-blobs' % new,
        'elasticbeanstalk-%s-files' % new,
        'elasticbeanstalk-%s-wfoutput' % new,
        'elasticbeanstalk-%s-system' % new,
    ]
    old_buckets = [
        'elasticbeanstalk-%s-blobs' % old,
        'elasticbeanstalk-%s-files' % old,
        'elasticbeanstalk-%s-wfoutput' % old,
    ]
    s3 = boto3.client('s3', region_name=REGION)
    for bucket in new_buckets:
        try:
            s3.create_bucket(Bucket=bucket)
        except:  # noqa: E722
            PRINT("bucket %s already created..." % bucket)

    # now copy them
    # aws s3 sync s3://mybucket s3://backup-mybucket
    # get rid of system bucket
    new_buckets.pop()
    for old, new in zip(old_buckets, new_buckets):
        oldb = 's3://%s' % old
        newb = 's3://%s' % new
        PRINT('copying data from old %s to new %s' % (oldb, newb))
        subprocess.call(['aws', 's3', 'sync', oldb, newb])


def clone_beanstalk_command_line(old, new, prod=False, copy_s3=False):
    """
    Maybe useful command to clone an existing ElasticBeanstalk environment to
    a new one. Will create an Elasticsearch instance, s3 buckets, clone the
    existing RDS of the environment, and optionally copy s3 contents.
    Also adds the new EB url to Auth0 callback urls.
    Should be run exclusively via command line, as it requires manual input
    and subprocess calls of AWS command line tools.

    Note:
        The eb cli tool sets up a configuration file in the directory of the
        project respository. As such, this command MUST be called from that
        directory. Will exit if not called from an eb initialized directory.

    Args:
        old (str): environment name of existing ElasticBeanstalk
        new (str): new ElasticBeanstalk environment name
        prod (bool): set to True if this is a prod environment. Default False
        copy_s3 (bool): set to True to copy s3 contents. Default False

    Returns:
        None
    """
    if 'Auth0Client' not in os.environ or 'Auth0Secret' not in os.environ:
        PRINT('Must set Auth0Client and Auth0Secret env variables! Exiting...')
        return
    PRINT('### eb status (START)')
    eb_ret = subprocess.call(['eb', 'status'])
    if eb_ret != 0:
        PRINT('This command must be called from an eb initialized repo! Exiting...')
        return
    PRINT('### eb status (END)')
    name = prompt_for_input(f"This will create an environment named {new}, cloned from {old}."
                            f" This includes s3, ES, RDS, and Auth0 callbacks."
                            f" If you are sure, type the new env name to confirm: ")
    if str(name) != new:
        PRINT('Could not confirm env. Exiting...')
        return
    PRINT('### start build ES service')
    add_es(new)
    PRINT('### create the s3 buckets')
    create_s3_buckets(new)
    PRINT('### create snapshot and copy database')
    db_endpoint = snapshot_and_clone_db(old, new)
    PRINT('### waiting for ES service')
    es_endpoint = get_es_build_status(new)
    PRINT('### clone elasticbeanstalk envrionment')
    # TODO, can we pass in github commit id here?
    clone_bs_env_cli(old, new, prod, db_endpoint, es_endpoint)
    PRINT('### allow auth-0 requests')
    add_to_auth0_client(new)
    if copy_s3 is True:
        PRINT('### copy contents of s3')
        copy_s3_buckets(new, old)
    PRINT('### All done! It may take some time for the beanstalk env to finish'
          ' initialization. You may want to deploy the most current FF branch.')


def delete_beanstalk_command_line(env):
    """
    Maybe useful command to delete an existing ElasticBeanstalk environment,
    including associated ES, s3, and RDS resources. Will also remove the
    associated callback url from Auth0.
    Should be run exclusively via command line, as it requires manual input
    and subprocess calls of AWS command line tools.

    Note:
        The eb cli tool sets up a configuration file in the directory of the
        project respository. As such, this command MUST be called from that
        directory. Will exit if not called from an eb initialized directory.

    Args:
        env (str): EB environment name to delete

    Returns:
        None
    """
    if 'Auth0Client' not in os.environ or 'Auth0Secret' not in os.environ:
        PRINT('Must set Auth0Client and Auth0Secret env variables! Exiting...')
        return
    PRINT('### eb status (START)')
    eb_ret = subprocess.call(['eb', 'status'])
    if eb_ret != 0:
        PRINT('This command must be called from an eb initialized repo! Exiting...')
        return
    PRINT('### eb status (END)')
    name = prompt_for_input("This will totally blow away the environment,"
                            " including s3, ES, RDS, and Auth0 callbacks."
                            " If you are sure, type the env name to confirm: ")
    if str(name) != env:
        PRINT('Could not confirm env. Exiting...')
        return
    PRINT('### Removing access to auth0')
    remove_from_auth0_client(env)
    PRINT('### Deleting beanstalk enviornment')
    delete_bs_env_cli(env)
    PRINT('### Delete contents of s3')
    delete_s3_buckets(env)
    PRINT('### Delete es domain')
    delete_es_domain(env)
    PRINT('### Delete database')
    delete_db(env)
    PRINT('### All done!')
