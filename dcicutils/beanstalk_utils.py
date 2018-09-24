'''
given and env in beanstalk do the follow
2. backup database
1. clone the existing environment to new beanstalk
   eb clone
3. set env variables on new beanstalk to point to database backup
4. for each s3bucket in existing environment:
    a.  create new bucket with proper naming
    b.  move files from existing bucket to new bucket
5. new ES instance?  (probably not covered by this script yet)
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
from dcicutils import ff_utils
from botocore.exceptions import ClientError

logger = logging.getLogger('logger')
logger.setLevel(logging.INFO)

FOURSIGHT_URL = 'https://foursight.4dnucleome.org/api/'
GOLDEN_DB = "fourfront-webprod.co3gwj7b7tpq.us-east-1.rds.amazonaws.com"
REGION = 'us-east-1'

# TODO: Maybe
'''
class EnvConfigData(OrderedDictionary):

    def to_aws_bs_options()

    def get_val_for_env()

    def is_data()

    def is_staging()

    def url()

    def bucket()

    def buckets()

    def part(self, componenet_name):
        self.get(componenet_name)

    def db(self):
        return self.part('db')

    def es(self):
        return self.part('es')

    def foursight(self):
        return self.part('foursight')

    def higlass(self):
        return self.part('higlass')
'''


class WaitingForBoto3(Exception):
    pass


def delete_db(db_identifier, take_snapshot=True):
    client = boto3.client('rds', region_name=REGION)
    if take_snapshot:
        try:
            resp = client.delete_db_instance(
                DBInstanceIdentifier=db_identifier,
                SkipFinalSnapshot=False,
                FinalDBSnapshotIdentifier=db_identifier + "-final"
            )
        except:  # noqa: E722
            # try without the snapshot
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


def get_health_page_info(bs_url):
    """
    Different use cases than ff_utils.get_health_page (that one is oriented
    towards external API usage and this one is more internal)
    """
    if not bs_url.endswith('/'):
        bs_url += "/"
    if not bs_url.startswith('http'):
        bs_url = 'http://' + bs_url

    health_res = requests.get(bs_url + 'health?format=json')
    return health_res.json()


# TODO: think about health page query parameter to get direct from config
def get_es_from_health_page(bs_url):
    health = get_health_page_info(bs_url)
    es = health['elasticsearch'].strip(':80')
    return es


def get_es_from_bs_config(env):
    bs_env = get_bs_env(env)
    for item in bs_env:
        if item.startswith('ES_URL'):
            return item.split('=')[1].strip(':80')


def is_indexing_finished(bs, version=None):
    is_beanstalk_ready(bs)
    bs_url = get_beanstalk_real_url(bs)
    if not bs_url.endswith('/'):
        bs_url += "/"
    # server not up yet
    try:
        # check to see if our version is updated
        if version:
            info = beanstalk_info(bs)
            if version == info.get('VersionLabel'):
                raise Exception("Beanstalk version has not updated from %s" % version)

        health_res = ff_utils.authorized_request(bs_url + 'counts?format=json', ff_env=bs)
        totals = health_res.json().get('db_es_total').split()

        # DB: 74048 ES: 74048 parse totals
        db_total = totals[1]
        es_total = totals[3]

        if int(db_total) > int(es_total):
            status = False
        else:
            status = True
    except Exception as e:
        print(e)
        status = False
        totals = 0

    return status, totals


def swap_cname(src, dest):
    # TODO clients should be global functions
    client = boto3.client('elasticbeanstalk', region_name=REGION)

    client.swap_environment_cnames(SourceEnvironmentName=src,
                                   DestinationEnvironmentName=dest)
    import time
    print("waiting for swap environment cnames")
    time.sleep(10)
    client.restart_app_server(EnvironmentName=src)
    client.restart_app_server(EnvironmentName=dest)


def whodaman():
    '''
    determines which evironment is currently hosting data.4dnucleome.org
    '''
    magic_cname = 'fourfront-webprod.9wzadzju3p.us-east-1.elasticbeanstalk.com'

    client = boto3.client('elasticbeanstalk', region_name=REGION)
    res = client.describe_environments(ApplicationName="4dn-web")
    logger.info(res)
    for env in res['Environments']:
        logger.info(env)
        if env.get('CNAME') == magic_cname:
            # we found data
            return env.get('EnvironmentName')


def beanstalk_config(env, appname='4dn-web'):
    client = boto3.client('elasticbeanstalk', region_name=REGION)
    return client.describe_configuration_settings(EnvironmentName=env,
                                                  ApplicationName=appname)


def beanstalk_info(env):
    client = boto3.client('elasticbeanstalk', region_name=REGION)
    res = client.describe_environments(EnvironmentNames=[env])

    return res['Environments'][0]


def get_beanstalk_real_url(env):
    """
    Return the real url for the elasticbeanstalk with given environment name.
    Name can be 'data', 'staging', or an actual environment.

    This function handles API throttling to AWS, so it should be used for all
    cases of getting the env name
    """
    url = ''
    urls = {'staging': 'http://staging.4dnucleome.org',
            'data': 'https://data.4dnucleome.org'}

    if env in urls:
        return urls[env]

    # times to wait on a throttling error. Keep 5 min lambda limit in mind
    for retry in [1, 2, 4, 6, 8, 10, 12, 14, 16, 18, 20]:
        try:
            if 'webprod' in env:
                data_env = whodaman()

                if data_env == env:
                    url = urls['data']
                else:
                    url = urls['staging']
            else:
                bs_info = beanstalk_info(env)
                url = "http://" + bs_info['CNAME']
        except ClientError as e:
            print('Client exception encountered while getting BS info for %s. Error: %s' % (env, str(e)))
            time.sleep(retry)
        except Exception as e:
            print('Unhandled exception encountered while getting BS info for %s. Error: %s' % (env, str(e)))
            raise e
        else:
            break
    return url


def is_beanstalk_ready(env):
    client = boto3.client('elasticbeanstalk', region_name=REGION)
    res = client.describe_environments(EnvironmentNames=[env])

    status = res['Environments'][0]['Status']
    if status != 'Ready':
        raise WaitingForBoto3("Beanstalk enviornment status is %s" % status)

    return status, 'http://' + res['Environments'][0].get('CNAME')


def is_snapshot_ready(snapshot_name):
    client = boto3.client('rds', region_name=REGION)
    resp = client.describe_db_snapshots(DBSnapshotIdentifier=snapshot_name)
    status = resp['DBSnapshots'][0]['Status']
    return status.lower() == 'available', resp['DBSnapshots'][0]['DBSnapshotIdentifier']


def is_es_ready(es_name):
    es = boto3.client('es', region_name=REGION)
    describe_resp = es.describe_elasticsearch_domain(DomainName=es_name)
    endpoint = describe_resp['DomainStatus'].get('Endpoint', None)
    status = True
    if endpoint is None:
        status = False
    else:
        endpoint = endpoint + ":80"
    return status, endpoint


def is_db_ready(snapshot_name):
    client = boto3.client('rds', region_name=REGION)
    status = False
    resp = client.describe_db_instances(DBInstanceIdentifier=snapshot_name)
    details = resp
    endpoint = resp['DBInstances'][0].get('Endpoint')
    if endpoint and endpoint.get('Address'):
        print("we got an endpoint:", endpoint['Address'])
        details = endpoint['Address']
        status = True

    return status, details


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


def create_db_from_snapshot(db_name, snapshot_name=None):
    if not snapshot_name:
        snapshot_name = db_name
    client = boto3.client('rds', region_name=REGION)
    try:
        response = client.restore_db_instance_from_db_snapshot(
                DBInstanceIdentifier=db_name,
                DBSnapshotIdentifier=snapshot_name,
                DBInstanceClass='db.t2.medium')
    except ClientError:
        # drop target database no backup
        try:
            delete_db(db_name, True)
        except ClientError:
            pass
        return "Deleting"

    return response['DBInstance']['DBInstanceArn']


def is_travis_finished(build_id):
    travis_key = os.environ.get('travis_key')
    status = False
    details = 'build not done or not found'
    headers = {'Content-Type': 'application/json',
               'Accept': 'application/json',
               'Travis-API-Version': '3',
               'User-Agent': 'tibanna/0.1.0',
               'Authorization': 'token %s' % travis_key
               }

    url = 'https://api.travis-ci.org/build/%s' % build_id

    logger.info("url: %s" % url)
    resp = requests.get(url, headers=headers)
    logger.info(resp.text)
    state = resp.json()['state']
    if resp.ok and state == 'failed':
        raise Exception('Build Failed')
    elif resp.ok and state == 'passed':
        status = True
        details = resp.json()

    return status, details


def snapshot_db(db_identifier, snapshot_name):
    client = boto3.client('rds', region_name=REGION)
    try:
        response = client.create_db_snapshot(
             DBSnapshotIdentifier=snapshot_name,
             DBInstanceIdentifier=db_identifier)
    except ClientError:
        # probably the guy already exists
        client.delete_db_snapshot(DBSnapshotIdentifier=snapshot_name)
        response = client.create_db_snapshot(
             DBSnapshotIdentifier=snapshot_name,
             DBInstanceIdentifier=db_identifier)
    print("Response from create db snapshot", response)
    print("waiting for snapshot to create")
    waiter = client.get_waiter('db_snapshot_completed')
    waiter.wait(DBSnapshotIdentifier=snapshot_name)
    print("done waiting, let's create a new database")
    try:
        response = client.restore_db_instance_from_db_snapshot(
                DBInstanceIdentifier=snapshot_name,
                DBSnapshotIdentifier=snapshot_name,
                DBInstanceClass='db.t2.medium')
    except ClientError:
        # drop target database
        delete_db(snapshot_name)

    waiter = client.get_waiter('db_instance_available')
    print("waiting for db to be restore... this might take some time")
    # waiter.wait(DBInstanceIdentifier=snapshot_name)
    # This doesn't mean the database is done creating, but
    # we now have enough information to continue to the next step
    endpoint = ''
    while not endpoint:
        resp = client.describe_db_instances(DBInstanceIdentifier=snapshot_name)
        endpoint = resp['DBInstances'][0].get('Endpoint')
        if endpoint and endpoint.get('Address'):
            print("we got an endpoint:", endpoint['Address'])
            return endpoint['Address']
        print(".")
        time.sleep(10)


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
    client = boto3.client('elasticbeanstalk', region_name=REGION)

    data = client.describe_configuration_settings(EnvironmentName=envname,
                                                  ApplicationName='4dn-web')
    options = data['ConfigurationSettings'][0]['OptionSettings']
    env_vars = [option['Value'] for option in options
                if option['OptionName'] == 'EnvironmentVariables'][0]
    return env_vars.split(',')


def update_bs_config(envname, template, keep_env_vars=False):
    client = boto3.client('elasticbeanstalk', region_name=REGION)

    # get important env variables
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
    client = boto3.client('elasticbeanstalk', region_name=REGION)

    template = 'fourfront-base'
    if for_indexing:
        template = 'fourfront-indexing'
    load_value = 'load_test_data'
    if load_prod:
        load_value = 'load_prod_data'
    options = [make_envvar_option('RDS_HOSTNAME', db_endpoint),
               make_envvar_option('ENV_NAME', envname),
               make_envvar_option('ES_URL', es_url),
               make_envvar_option('LOAD_FUNCTION', load_value)
               ]
    try:
        res = client.create_environment(ApplicationName='4dn-web',
                                        EnvironmentName=envname,
                                        TemplateName=template,
                                        OptionSettings=options,
                                        )
    except ClientError:
        # already exists update it
        res = client.update_environment(EnvironmentName=envname,
                                        TemplateName=template,
                                        OptionSettings=options)
    return res


def clone_bs_env(old, new, load_prod, db_endpoint, es_url):
    env = 'RDS_HOSTNAME=%s,ENV_NAME=%s,ES_URL=%s' % (db_endpoint, new, es_url)
    if load_prod is True:
        env += ",LOAD_FUNCTION=load_prod_data"
    subprocess.check_call(['./eb', 'clone', old, '-n', new,
                           '--envvars', env,
                           '--exact', '--nohang'])


def log_to_foursight(event, lambda_name, status='WARN', full_output=None):
    fs = event.get('_foursight')
    if not full_output:
        full_output = '%s started to run' % lambda_name
    if fs:
        data = {'status': status,
                'description': fs.get('log_desc'),
                'full_output': full_output
                }
        ff_auth = os.environ.get('FS_AUTH')
        headers = {'content-type': "application/json",
                   'Authorization': ff_auth}
        url = FOURSIGHT_URL + 'checks/' + fs.get('check')
        res = requests.put(url, data=json.dumps(data), headers=headers)
        print(res.text)
        return res


def create_foursight_auto(dest_env):
    fs = {'dest_env': dest_env}

    # whats our url
    fs['bs_url'] = get_beanstalk_real_url(dest_env)
    fs['fs_url'] = get_foursight_env(dest_env, fs['bs_url'])
    fs['es_url'] = get_es_from_bs_config(dest_env)

    fs['foursight'] = create_foursight(**fs)
    if fs['foursight'].get('initial_checks'):
        del fs['foursight']['initial_checks']

    return fs


def get_foursight_env(dest_env, bs_url=None):

    if not bs_url:
        bs_url = get_beanstalk_real_url(dest_env)

    env = dest_env
    if 'data.4dnucleome.org' in bs_url:
        env = 'data'
    elif 'staging.4dnucleome.org' in bs_url:
        env = 'staging'

    return env


def create_foursight(dest_env, bs_url, es_url, fs_url=None):
    '''
    creates a new environment on foursight to be used for monitoring
    '''

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
    if "-" in fs_url:
        fs_url = fs_url.split("-")[1]

    foursight_url = FOURSIGHT_URL + 'environments/' + fs_url
    payload = {"fourfront": bs_url,
               "es": es_url,
               "ff_env": dest_env,
               }
    logger.info("Hitting up Foursight url %s with payload %s" %
                (foursight_url, json.dumps(payload)))

    ff_auth = os.environ.get('FS_AUTH')
    headers = {'content-type': "application/json",
               'Authorization': ff_auth}
    res = requests.put(foursight_url,
                       data=json.dumps(payload),
                       headers=headers)
    try:
        return res.json()
    except:  # noqa: E722
        raise Exception(res.text)


def create_s3_buckets(new):
    new_buckets = [
        'elasticbeanstalk-%s-blobs' % new,
        'elasticbeanstalk-%s-files' % new,
        'elasticbeanstalk-%s-wfoutput' % new,
        'elasticbeanstalk-%s-system' % new,
    ]
    s3 = boto3.client('s3', region_name=REGION)
    for bucket in new_buckets:
        s3.create_bucket(Bucket=bucket)


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


def add_to_auth0_client(new):
    # first get the url of the newly created beanstalk environment
    eb = boto3.client('elasticbeanstalk', region_name=REGION)
    env = eb.describe_environments(EnvironmentNames=[new])
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


def sizeup_es(new):
    es = boto3.client('es', region_name=REGION)
    resp = es.update_elasticsearch_domain_config(
        DomainName=new,
        ElasticsearchClusterConfig={
            'InstanceType': 'm3.xlarge.elasticsearch',
            'InstanceCount': 4,
            'DedicatedMasterEnabled': True,
        }
    )

    print(resp)


def add_es(new, force_new=False, kill_indices=False):
    es = boto3.client('es', region_name=REGION)
    if force_new:
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


def create_new_es(new):
    es = boto3.client('es', region_name=REGION)
    resp = es.create_elasticsearch_domain(
        DomainName=new,
        ElasticsearchVersion='5.3',
        ElasticsearchClusterConfig={
            'InstanceType': 'm4.large.elasticsearch',
            'InstanceCount': 3,
            'DedicatedMasterEnabled': False,
        },
        EBSOptions={"EBSEnabled": True, "VolumeType": "standard", "VolumeSize": 10},
        AccessPolicies=json.dumps({
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {
                        "AWS": "arn:aws:iam::643366669028:role/Developer"
                    },
                    "Action": [
                        "es:*"
                    ],
                    "Condition": {
                        "IpAddress": {
                            "aws:SourceIp": [
                                "0.0.0.0/0",
                                "134.174.140.197/32",
                                "134.174.140.208/32",
                                "172.31.16.84/32",
                                "172.31.73.1/24",
                                "172.31.77.1/24"
                            ]
                        }
                    },
                }
            ]
        })
    )
    print(resp)
    return resp


def get_es_build_status(new):
    # get the status of this bad boy
    es = boto3.client('es', region_name=REGION)
    endpoint = None
    while endpoint is None:
        describe_resp = es.describe_elasticsearch_domain(DomainName=new)
        endpoint = describe_resp['DomainStatus'].get('Endpoint')
        if endpoint is None:
            print(".")
            time.sleep(10)

    print(endpoint)

    # aws uses port 80 for es connection, lets be specific
    return endpoint + ":80"


def eb_deploy(new):
    subprocess.check_call(['eb', 'deploy', new])


def main():
    parser = argparse.ArgumentParser(
        description="Clone a beanstalk env into a new one",
        )
    parser.add_argument('--old')
    parser.add_argument('--new')
    parser.add_argument('--prod', action='store_true', default=False, help='load prod data on new env?')
    parser.add_argument('--deploy_current', action='store_true', help='deploy current branch')
    parser.add_argument('--skips3', action='store_true', default=False,
                        help='skip copying files from s3')

    parser.add_argument('--onlys3', action='store_true', default=False,
                        help='skip copying files from s3')

    args = parser.parse_args()
    if args.onlys3:
        print("### only copy contents of s3")
        copy_s3_buckets(args.new, args.old)
        return

    print("### start build ES service")
    add_es(args.new)
    print("### create the s3 buckets")
    create_s3_buckets(args.new)
    print("### copy database")
    db_endpoint = snapshot_db(args.old, args.new)
    print("### waiting for ES service")
    es_endpoint = get_es_build_status(args.new)
    print("### clone elasticbeanstalk envrionment")
    # TODO, can we pass in github commit id here?
    clone_bs_env(args.old, args.new, args.prod, db_endpoint, es_endpoint)
    print("### allow auth-0 requests")
    add_to_auth0_client(args.new)
    if not args.skips3:
        print("### copy contents of s3")
        copy_s3_buckets(args.new, args.old)
    if args.deploy_current:
        print("### deploying local code to new eb environment")
        eb_deploy(args.new)

    print("all set, it may take some time for the beanstalk env to finish starting up")


if __name__ == "__main__":
    main()
