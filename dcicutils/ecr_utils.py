import boto3
import base64
from .misc_utils import PRINT
from .env_utils import is_cgap_env


# Defines the standard Docker image tags for CGAP ECR
# Latest and stable - something like this should be reasonable?
CGAP_ECR_LAYOUT = {
    'latest-wsgi': 'Latest version of WSGI Application',
    'latest-indexer': 'Latest version of Indexer Application',
    'latest-ingester': 'Latest version of the Ingester Application',
    'stable-wsgi': 'Stable version of WSGI Application',
    'stable-indexer': 'Stable version of Indexer Application',
    'stable-ingester': 'Stable version of the Ingester Application'
}
CGAP_ECR_REGION = 'us-east-1'


class ECRUtils(object):
    """ Utility class for interacting with ECR.
        Initialized with an env name, from which a repo URL is resolved.

        NOTE 1: unlike s3Utils, the resolved resources (repository) will NOT be
        created if it does not already exist.
        NOTE 2: the (already created) ECR repository must have the env_name in the URI
    """

    def __init__(self, *, env_name='cgap-mastertest', local_repository='cgap-wsgi'):
        """ Creates an ECR client on startup """
        self.env = env_name
        self.local_repository = local_repository
        self.is_cgap = is_cgap_env(env_name)
        if not self.is_cgap:
            raise NotImplementedError('ECR setup is not implemented for fourfront!')
        self.client = boto3.client('ecr', region_name=CGAP_ECR_REGION)  # XXX: constant?
        self.url = None  # set by calling the below method

    def resolve_repository_uri(self, url=None):
        if not self.url or url:
            PRINT('NOTE: Calling out to ECR')
            try:
                resp = self.client.describe_repositories()
                for repo in resp.get('repositories', []):
                    if repo['repositoryUri'].endswith(self.env):
                        url = repo['repositoryUri']
            except Exception as e:
                PRINT('Could not retrieve repository information from ECR: %s' % e)
        self.url = url  # hang onto this
        return url

    def get_uri(self):
        """ Returns URI if it's been set, raise exception otherwise """
        if self.url is not None:
            return self.url
        raise Exception('Tried to get URI when it has not been resolved yet!')

    def authorize_user(self):
        """ Calls to boto3 to get authorization credentials for ECR.
            Gives a result like this:
                {
                    'authorizationData': [{'authorizationToken': 'very long token....',
                                            'expiresAt': <datetime>,
                                            'proxyEndpoint': <ecr URL> ... }]
                }
        """
        try:
            [auth_data] = self.client.get_authorization_token()['authorizationData']
            return auth_data
        except Exception as e:
            PRINT('Could not acquire ECR authorization credentials: %s' % e)
            raise

    @staticmethod
    def extract_ecr_password_from_authorization(*, authorization):
        """ Extracts the password from the authorization token returned from the
            above API call.
        """
        return base64.b64decode(authorization['authorizationToken']).replace(b'AWS:', b'').decode('utf-8')
