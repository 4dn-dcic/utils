from elasticsearch import Elasticsearch, RequestsHttpConnection
from aws_requests_auth.boto_utils import BotoAWSRequestsAuth
import curator


def get_index_list(client, name, days_old=0, timestring='%Y.%m.%d', ilo=None):
    if ilo is None:
        ilo = curator.IndexList(client)

    ilo.filter_by_regex(kind='prefix', value=name)
    ilo.filter_by_age(source='name', direction='older', timestring=timestring, unit='days',
                      unit_count=days_old)
    return ilo


def create_es_client(es_url, use_aws_auth=False):
    if isinstance(es_url, (list, tuple)):
        addresses = es_url
    else:
        addresses = [es_url, ]

    es_options = {'retry_on_timeout': True,
                  'maxsize': 50  # parallellism...
                  }
    if use_aws_auth:
        host = addresses[0].split('//')
        host = host[-1].split(":")
        auth = BotoAWSRequestsAuth(aws_host=host[0].rstrip('/'),
                                   aws_region='us-east-1',
                                   aws_service='es')
        es_options['connection_class'] = RequestsHttpConnection
        es_options['http_auth'] = auth

    return Elasticsearch(addresses, **es_options)


def create_snapshot_repo(client, repo_name,  s3_bucket):
    snapshot_body = {'type': 's3',
                     'settings': {
                         'bucket': s3_bucket,
                         'region': 'us-east-1',
                         'role_arn': 'arn:aws:iam::643366669028:role/S3Roll'
                      }
                     }
    return client.snapshot.create_repository(repository=repo_name, body=snapshot_body)
