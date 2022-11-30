import logging
from opensearchpy import OpenSearch
from dcicutils.es_utils import ElasticSearchServiceClient, prepare_es_options


logging.basicConfig()
logger = logging.getLogger('logger')
logger.setLevel(logging.INFO)


class OpenSearchServiceClient(ElasticSearchServiceClient):
    pass  # this should not differ since we are using boto3 API


def create_os_client(os_url: str, use_aws_auth=True, **options) -> OpenSearch:
    """
    Use to create an OpenSearch client - needed for communication with
    AWS OpenSearch Serverless. Previous versions of OpenSearch up to 2.3
    will work with the existing es_utils but the serverless version
    does not.

    :param os_url: URL to OpenSearch
    :param use_aws_auth: sign requests with AWSV4 signature (required typically)
    :param options: any additional client options, see opensearch-py documentation
    :return: an OpenSearch client
    """
    os_options = prepare_es_options(os_url, use_aws_auth, **options)
    return OpenSearch(os_url, **os_options)
