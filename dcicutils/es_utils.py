from elasticsearch import Elasticsearch, RequestsHttpConnection
from aws_requests_auth.boto_utils import BotoAWSRequestsAuth

'''
info about snapshots on AWS
https://docs.aws.amazon.com/elasticsearch-service/latest/developerguide/es-managedomains-snapshots.html
'''


def create_es_client(es_url, use_aws_auth=True, **options):
    """
    Use to create a ES that supports the signature version 4 signing process.
    Need to do role-based IAM access control for AWS hosted ES.
    Takes a string es server url, boolean whether or not to use aws auth
    signing procedure, and any additional kwargs that will be passed to
    creation of the Elasticsearch client.
    """
    # may be passed in as a list, as was done previously
    if isinstance(es_url, (list, tuple)):
        es_url = es_url[0]

    # default options
    es_options = {'retry_on_timeout': True,
                  'maxsize': 50,  # parallellism...
                  'connection_class': RequestsHttpConnection}
    if use_aws_auth:
        host = es_url.split('//')  # remove schema from url
        host = host[-1].split(":")
        auth = BotoAWSRequestsAuth(aws_host=host[0].rstrip('/'),
                                   aws_region='us-east-1',
                                   aws_service='es')
        es_options['http_auth'] = auth
    es_options.update(**options)  # add any given keyword options at the end

    return Elasticsearch(es_url, **es_options)


def get_index_list(client, name, days_old=0, timestring='%Y.%m.%d', ilo=None):
    return []
    # import curator
    # if ilo is None:
    #     ilo = curator.IndexList(client)
    #
    # ilo.filter_by_regex(kind='prefix', value=name)
    # ilo.filter_by_age(source='name', direction='older', timestring=timestring, unit='days',
    #                   unit_count=days_old)
    # return ilo


def create_snapshot_repo(client, repo_name,  s3_bucket):
    snapshot_body = {'type': 's3',
                     'settings': {
                         'bucket': s3_bucket,
                         'region': 'us-east-1',
                         'role_arn': 'arn:aws:iam::643366669028:role/S3Roll'
                      }
                     }
    return client.snapshot.create_repository(repository=repo_name, body=snapshot_body)
