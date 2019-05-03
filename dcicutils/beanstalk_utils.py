'''
Utilities related to ElasticBeanstalk deployment and management.
This includes, but is not limited to: ES, s3, RDS, Auth0, and Foursight.
'''
from __future__ import print_function
import subprocess
import logging
import argparse
import boto3
import os
import json
import requests
import time
from datetime import datetime
from dcicutils import ff_utils
from botocore.exceptions import ClientError

logging.basicConfig()
logger = logging.getLogger('logger')
logger.setLevel(logging.INFO)

FOURSIGHT_URL = 'https://foursight.4dnucleome.org/api/'
# magic CNAME corresponds to data.4dnucleome
MAGIC_CNAME = 'fourfront-webprod.9wzadzju3p.us-east-1.elasticbeanstalk.com'
GOLDEN_DB = "fourfront-webprod.co3gwj7b7tpq.us-east-1.rds.amazonaws.com"
REGION = 'us-east-1'


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
    # safety. Do not allow accidental programmatic deletion of webprod DB
    if 'webprod' in db_identifier and not allow_delete_prod:
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
        except:
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
    print(resp)
    return resp


def get_es_from_bs_config(env):
    """
    Given an ElasticBeanstalk environment name, get the corresponding
    Elasticsearch url from the EB configuration

    Args:
        env (str): ElasticBeanstalk environment name

    Returns:
        str: Elasticsearch url
    """
    bs_env = get_bs_env(env)
    for item in bs_env:
        if item.startswith('ES_URL'):
            return item.split('=')[1].strip(':80')


def is_indexing_finished(bs, prev_version=None):
    """
    Checker function used with torb waitfor lambda; output must be standarized.
    Check to see if indexing of a Fourfront environment corresponding to given
    ElasticBeanstalk environment is finished by looking at the /counts page.

    Args:
        bs (str): ElasticBeanstalk environment name
        prev_version (str): optional EB version of the previous configuration

    Returns:
        bool, list: True if done, results from /counts page

    Raises:
        Exception: if EB environment VersionLabel is equal to prev_version
    """
    # is_beanstalk_ready will raise WaitingForBoto3 if not ready
    is_beanstalk_ready(bs)
    bs_url = get_beanstalk_real_url(bs)
    if not bs_url.endswith('/'):
        bs_url += "/"
    # server not up yet
    try:
        # check to see if our version is updated
        if prev_version:
            info = beanstalk_info(bs)
            if prev_version == info.get('VersionLabel'):
                raise Exception("Beanstalk version has not updated from %s"
                                % prev_version)

        health_res = ff_utils.authorized_request(bs_url + 'counts?format=json',
                                                 ff_env=bs)
        totals = health_res.json().get('db_es_total').split()

        # example value of split totals: ["DB:", "74048", "ES:", "74048"]
        db_total = totals[1]
        es_total = totals[3]

        if int(db_total) > int(es_total):
            is_ready = False
        else:
            is_ready = True
    except Exception as exc:
        print('Error on is_indexing_finished: %s' % exc)
        is_ready = False
        totals = []

    return is_ready, totals


def swap_cname(src, dest):
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
    printing("Swapping CNAMEs %s and %s..." % (src, dest))
    client.swap_environment_cnames(SourceEnvironmentName=src,
                                   DestinationEnvironmentName=dest)
    print("Giving CNAMEs 10 seconds to update...")
    time.sleep(10)
    print("Restarting app servers for %s and %s..." % (src, dest))
    client.restart_app_server(EnvironmentName=src)
    client.restart_app_server(EnvironmentName=dest)


def whodaman():
    '''
    Determines which ElasticBeanstalk environment is currently hosting
    data.4dnucleome.org. Requires IAM permissions for EB!

    Returns:
        str: EB environment name hosting data.4dnucleome
    '''
    client = boto3.client('elasticbeanstalk', region_name=REGION)
    res = describe_beanstalk_environments(client, ApplicationName="4dn-web")
    logger.info(res)
    for env in res['Environments']:
        logger.info(env)
        if env.get('CNAME') == MAGIC_CNAME:
            # we found data
            return env.get('EnvironmentName')


def beanstalk_config(env, appname='4dn-web'):
    client = boto3.client('elasticbeanstalk', region_name=REGION)
    return client.describe_configuration_settings(EnvironmentName=env,
                                                  ApplicationName=appname)


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
    return res['Environments'][0]


def get_beanstalk_real_url(env):
    """
    Return the real url for the elasticbeanstalk with given environment name.
    Name can be 'data', 'staging', or an actual environment.

    Args:
        env (str): ElasticBeanstalk environment name

    Returns:
        str: url of the ElasticBeanstalk environment
    """
    url = ''
    urls = {'staging': 'http://staging.4dnucleome.org',
            'data': 'https://data.4dnucleome.org'}

    if env in urls:
        return urls[env]

    if 'webprod' in env:
        data_env = whodaman()

        if data_env == env:
            url = urls['data']
        else:
            url = urls['staging']
    else:
        bs_info = beanstalk_info(env)
        url = "http://" + bs_info['CNAME']

    return url


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


def describe_beanstalk_environments(client, **kwargs):
    """
    Generic function for retrying client.describe_environments to avoid
    AWS throttling errors
    Passes all given kwargs to client.describe_environments
    """
    env_info = kwargs.get('EnvironmentNames', kwargs.get('ApplicationName', 'Unknown environment'))
    for retry in [1, 1, 1, 1, 2, 2, 2, 4, 4, 6, 8, 10, 12, 14, 16, 18, 20]:
        try:
            res = client.describe_environments(**kwargs)
        except ClientError as e:
            print('Client exception encountered while getting BS info for %s. Error: %s' % (env_info, str(e)))
            time.sleep(retry)
        except Exception as e:
            print('Unhandled exception encountered while getting BS info for %s. Error: %s' % (env_info, str(e)))
            raise e
        else:
            return res
    raise Exception('Could not describe Beanstalk environments due ClientErrors, likely throttled connections.')


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
    is_ready = status.lower() == 'available'
    return is_ready, resp['DBSnapshots'][0]['DBSnapshotIdentifier']


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


def create_db_from_snapshot(db_identifier, snapshot_name, delete_db=True):
    """
    Given an RDS instance indentifier and a snapshot ARN/name, create an RDS
    instance from the snapshot. If an existing instance already exists with
    the given identifier and delete_db is True, will attempt to delete it
    and return "Deleting". Otherwise, will just bail

    Args:
        db_identifier (str): RDS instance identifier
        snapshot_name (str): identifier/ARN of RDS snapshot to restore from


    Returns:
        str: resource ARN if successful, otherwise "Deleting"
    """
    client = boto3.client('rds', region_name=REGION)
    try:
        response = client.restore_db_instance_from_db_snapshot(
            DBInstanceIdentifier=db_identifier,
            DBSnapshotIdentifier=snapshot_name,
            DBInstanceClass='db.t2.medium',

        )
    except ClientError:
        # Something went wrong
        # Even if delete_db, never allow deletion of a db with 'webprod' in it
        if delete_db:
            # Drop target database with final snapshot
            try:
                delete_db(db_identifier, True)
            except ClientError:
                pass
            return "Deleting"
        else:
            return "Error"
    return response['DBInstance']['DBInstanceArn']


def is_travis_finished(build_id):
    """
    Checker function used with torb waitfor lambda; output must be standarized.
    Check to see if a given travis build has passed

    Args:
        build_id (str): Travis build identifier

    Returns:
        bool, dict: True if done, Travis response JSON

    Raises:
        Exception: if the Travis build failed
    """
    travis_key = os.environ.get('travis_key')
    is_ready = False
    details = 'build not done or not found'
    headers = {'Content-Type': 'application/json',
               'Accept': 'application/json',
               'Travis-API-Version': '3',
               'User-Agent': 'tibanna/0.1.0',
               'Authorization': 'token %s' % travis_key}

    url = 'https://api.travis-ci.org/build/%s' % build_id

    logger.info("Travis build url: %s" % url)
    resp = requests.get(url, headers=headers)
    logger.info("Travis build response: %s" % resp.text)
    state = resp.json()['state']
    if resp.ok and state == 'failed':
        raise Exception('Build Failed')
    elif resp.ok and state == 'passed':
        is_ready = True
        details = resp.json()

    return is_ready, details


def make_envvar_option(name, value):
    return {'Namespace': 'aws:elasticbeanstalk:application:environment',
            'OptionName': name,
            'Value': value
            }


def set_bs_env(envname, var, template=None):
    client = boto3.client('elasticbeanstalk', region_name=REGION)
    options = []

    try:
        # add default environment from existing env
        # allowing them to be overwritten by var
        env_vars = get_bs_env(envname)
        for evar in env_vars:
            k, v = evar.split('=')
            if var.get(k, None) is None:
                var[k] = v
    except:  # noqa: E722
        pass

    for key, val in var.iteritems():
        options.append(make_envvar_option(key, val))

    logging.info("About to update beanstalk with options as %s" % str(options))

    if template:
        return client.update_environment(EnvironmentName=envname,
                                         OptionSettings=options,
                                         TemplateName=template
                                         )
    else:
        return client.update_environment(EnvironmentName=envname,
                                         OptionSettings=options)


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


def update_bs_config(envname, template, keep_env_vars=False):
    """
    Update the configuration for an existing ElasticBeanstalk environment.
    Requires the environment name and a template to use. Can optionally
    keep all environment variables from the existing environment

    Args:
        envname (str): name of the EB environment
        template (str): configuration template to use
        keep_env_vars (bool): if True, keep existing env vars. Default False

    Returns:
        dict: update_environment response
    """
    client = boto3.client('elasticbeanstalk', region_name=REGION)

    # get important env variables from the current env and keep them
    if keep_env_vars:
        options = []
        env_vars = get_bs_env(envname)
        for var in env_vars:
            key, value = var.split('=')
            options.append(make_envvar_option(key, value))

        return client.update_environment(EnvironmentName=envname,
                                         TemplateName=template,
                                         OptionSettings=options)

    return client.update_environment(EnvironmentName=envname,
                                     TemplateName=template)


def create_bs(envname, load_prod, db_endpoint, es_url, for_indexing=False):
    """
    Create a beanstalk environment given an envname. Use customized options,
    configuration template, and environment variables. If adding new env vars,
    make sure to overwrite them here.
    If the environment already exists, will update it instead

    Args:
        envname (str): ElasticBeanstalk (EB) enviroment name
        load_prod (bool): sets the LOAD_FUNCTION EB env var
        db_endpoint (str): sets the RDS_HOSTNAME EB env var
        es_url (str): sets the ES_URL EB env var
        for_indexing (bool): If True, use 'fourfront-indexing' config template

    Returns:
        dict: boto3 res from create_environment/update_environment
    """
    client = boto3.client('elasticbeanstalk', region_name=REGION)

    # deterimine the configuration template for Elasticbeanstal
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
        # environment already exists update it
        res = client.update_environment(
            EnvironmentName=envname,
            TemplateName=template,
            OptionSettings=options
        )
    return res


def log_to_foursight(event, lambda_name='', overrides=None):
    """
    Use Foursight as a logging tool within in a lambda function by doing a PUT
    to /api/checks. Requires the the event has "_foursight" key, which is a
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
        print('Foursight response from %s: %s' % (url, res.text))
        try:
            return res.json()
        except Exception as exc:
            raise Exception('Error putting FS check to %s with body %s. '
                            'Exception: %s. Response text: %s'
                            % (url, data, exc, res.text))


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
    fs = {'dest_env': dest_env}

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
    if not bs_url.endswith("/"):
        bs_url += "/"

    es_url = es_url.rstrip(":80")
    if not es_url.startswith("http"):
        es_url = "https://" + es_url
    if not es_url.endswith("/"):
        es_url += "/"

    # environments on foursight don't include fourfront
    if not fs_url:
        fs_url = dest_env
    if fs_url.startswith('fourfront-'):
        fs_url = fs_url[len('fourfront-'):]

    foursight_url = FOURSIGHT_URL + 'environments/' + fs_url
    payload = {"fourfront": bs_url,  "es": es_url, "ff_env": dest_env}

    ff_auth = os.environ.get('FS_AUTH')
    headers = {'content-type': "application/json", 'Authorization': ff_auth}
    res = requests.put(foursight_url, data=json.dumps(payload), headers=headers)
    try:
        return res.json()
    except Exception as exc:
        raise Exception('Error creating FS environ to %s with body %s. '
                        'Exception: %s. Response text: %s'
                        % (foursight_url, payload, exc, res.text))


def create_new_es(new):
    """
    Create a new Elasticsearch domain with given name. See the
    args below for the settings used.

    TODO: do we want to add cognito and access policy setup here?
          I think not, since that's a lot of info to put out there...

    Args:
        new (str): Elasticsearch instance name

    Returns:
        dict: response from boto3 client
    """
    es = boto3.client('es', region_name=REGION)
    resp = es.create_elasticsearch_domain(
        DomainName=new,
        ElasticsearchVersion='5.3',
        ElasticsearchClusterConfig={
            'InstanceType': 'm4.xlarge.elasticsearch',
            'InstanceCount': 1,
            'DedicatedMasterEnabled': True,
            'DedicatedMasterType': 't2.small.elasticsearch',
            'DedicatedMasterCount': 3
        },
        EBSOptions={
            "EBSEnabled": True,
            "VolumeType": "standard",
            "VolumeSize": 100
        }
    )
    print('=== CREATED NEW ES INSTANCE %s ===' % new)
    print('MAKE SURE TO UPDATE COGNITO AND ACCESS POLICY USING THE GUI!')
    print(resp)

    return resp


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
            print(".")
            tries += 1
            time.sleep(10)

    # aws uses port 80 for es connection, lets be specific
    if endpoint:
        endpoint += ":80"
    print('Found ES endpoint for %s: %s' % (new, endpoint))
    return endpoint


###############################################################
# Functions meant to be used locally to clone a beanstalk ENV #
# A lot of these functions use command line tools.            #
# Sort of janky, but could end up being helpful someday ...   #
###############################################################


def add_es(new, force_new=False, kill_indices=False):
    """
    Either gets information on an existing Elasticsearch instance
    or, if force_new is True, will create a new instance.

    If not force_new, attempt to delete all indices if kill_indices=True

    Args:
        new (str): Fourfront EB environment name used for ES instance
        force_new (bool): if True, make a new ES. Default False
        kill_indices(bool): if True, delete all indices. Default False

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
        else:
            # now kill all the indexes
            if kill_indices:
                base = 'https://' + resp['DomainStatus']['Endpoint']
                url = base + '/_all'
                requests.delete(url)

    return resp['DomainStatus']['ARN']


def clone_bs_env(old, new, load_prod, db_endpoint, es_url):
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
    subprocess.check_call(['./eb', 'clone', old, '-n', new,
                           '--envvars', env,
                           '--exact', '--nohang'])

def create_s3_buckets(new):
    """
    Given an ElasticBeanstalk env name, create the following s3 buckets

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
    print("Response from create db snapshot: %s" % snap_res)
    print("Waiting for snapshot to create...")
    waiter = client.get_waiter('db_snapshot_completed')
    waiter.wait(DBSnapshotIdentifier=snapshot_name)
    print("Done waiting, creating a new database with name %s" % snapshot_name)
    db_res = create_db_from_snapshot(snapshot_name, snapshot_name, False)
    if db_res == 'Error':
        raise Exception('Could not create DB %s; already exists' % snapshot_name)
    print("Waiting for DB to be created from snapshot...")
    endpoint = ''
    while not endpoint:
        resp = client.describe_db_instances(DBInstanceIdentifier=snapshot_name)
        endpoint = resp['DBInstances'][0].get('Endpoint')
        if endpoint and endpoint.get('Address'):
            print("We got an endpoint:", endpoint['Address'])
            return endpoint['Address']
        print(".")
        time.sleep(10)


def add_to_auth0_client(new):
    # first get the url of the newly created beanstalk environment
    client = boto3.client('elasticbeanstalk', region_name=REGION)
    env = describe_beanstalk_environments(client, EnvironmentNames=[new])
    url = None
    print("waiting for beanstalk to be up, this make take some time...")
    while url is None:
        url = env['Environments'][0].get('CNAME')
        if url is None:
            print(".")
            time.sleep(10)
    auth0_client_update(url)

    # TODO: need to also update ES permissions policy with ip addresses of elasticbeanstalk
    # or configure application to use AWS IAM stuff


def auth0_client_update(url):
    # Auth0 stuff
    # generate a jwt to validate future requests
    client = os.environ.get("Auth0Client")
    secret = os.environ.get("Auth0Secret")

    payload = {"grant_type": "client_credentials",
               "client_id": client,
               "client_secret": secret,
               "audience": "https://hms-dbmi.auth0.com/api/v2/"}
    headers = {'content-type': "application/json"}
    res = requests.post("https://hms-dbmi.auth0.com/oauth/token",
                        data=json.dumps(payload),
                        headers=headers)

    print(res.json())
    jwt = res.json()['access_token']
    client_url = "https://hms-dbmi.auth0.com/api/v2/clients/%s" % client
    headers['authorization'] = 'Bearer %s' % jwt

    get_res = requests.get(client_url + '?fields=callbacks', headers=headers)

    callbacks = get_res.json()['callbacks']
    callbacks.append("http://" + url)
    client_data = {'callbacks': callbacks}

    update_res = requests.patch(client_url, data=json.dumps(client_data), headers=headers)
    print(update_res.json().get('callbacks'))


def copy_s3_buckets(new, old):
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

            print("bucket already created....")

    # now copy them
    # aws s3 sync s3://mybucket s3://backup-mybucket
    # get rid of system bucket
    new_buckets.pop()
    for old, new in zip(old_buckets, new_buckets):
        oldb = "s3://%s" % old
        newb = "s3://%s" % new
        print("copying data from old %s to new %s" % (oldb, newb))
        subprocess.call(['aws', 's3', 'sync', oldb, newb])


def clone_beanstalk_command_line(old, new, prod=False, copy_s3=False):
    """
    Maybe useful command to clone an existing ElasticBeanstalk environment to
    a new one. Will create an Elasticsearch instance, s3 buckets, clone the
    existing RDS of the environment, and optionally copy s3 contents.
    Also adds the new environment to Auth0 callback urls.

    Args:
        old (str): environment name of existing ElasticBeanstalk
        new (str): new ElasticBeanstalk environment name
        prod (bool): set to True if this is a prod environment. Default False
        copy_s3 (bool): set to True to copy s3 contents. Default False

    Returns:
        None
    """

    print("### start build ES service")
    add_es(new)
    print("### create the s3 buckets")
    create_s3_buckets(new)
    print("### create snapshot and copy database")
    db_endpoint = snapshot_and_clone_db(old, new)
    print("### waiting for ES service")
    es_endpoint = get_es_build_status(new)
    print("### clone elasticbeanstalk envrionment")
    # TODO, can we pass in github commit id here?
    clone_bs_env(old, new, prod, db_endpoint, es_endpoint)
    print("### allow auth-0 requests")
    add_to_auth0_client(new)
    if copy_s3 is True:
        print("### copy contents of s3")
        copy_s3_buckets(new, old)
    print("All done! It may take some time for the beanstalk env to finish "
          "initialization. You may want to deploy the most current FF branch.")
