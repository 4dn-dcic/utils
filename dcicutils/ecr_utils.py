import boto3
import base64
import os

from typing import List, Optional, Union
from .common import REGION as COMMON_REGION
from .misc_utils import PRINT


class ECRUtils(object):
    """ Utility class for interacting with ECR.
        Initialized with an env name, from which a repo URL is resolved.

        NOTE 1: unlike s3Utils, the resolved resources (repository) will NOT be
        created if it does not already exist.
        NOTE 2: the (already created) ECR repository must have the env_name in the URI
    """

    # In many cases, the ECR repo named 'main' is where images live.
    # There could be blue/green deploys or other multi-environment account situations where others are used.
    DEFAULT_ECS_REPOSITORY = 'main'

    REGION = COMMON_REGION  # this default must match what ecs_utils.ECSUtils and secrets_utils.assume_identity use

    # Typically only the recent images are what needs to be seen when doing things like adding or removing labels.
    # But this number is quite arbitrary. Since the command to show a list will summarize how many were not shown,
    # it should be easy to ask for a wider window.
    IMAGE_LIST_DEFAULT_COUNT_LIMIT = 10

    # This is extremely arbitrary. It shouldn't be a major efficiency matter since this is rarely used.
    # A small number means the feature of managing multiple chunks is regularly tested and less likely to have
    # weird bugs creep in that we don't notice until later.
    IMAGE_LIST_CHUNK_SIZE = 25

    # defaults were formerly env_name='cgap-mastertest', local_repository='cgap-wsgi'
    def __init__(self, *, env_name=None, local_repository=None, region=None, ecr_client=None,
                 ecs_repository=None):
        """ Creates an ECR client on startup """
        self.env = env_name or os.environ.get('ENV_NAME')
        self.local_repository = local_repository  # Not sure this is even used any more. Should we deprecate it?
        self.ecr_client = ecr_client or boto3.client('ecr', region_name=region or self.REGION)
        self.ecs_repository = ecs_repository or self.DEFAULT_ECS_REPOSITORY
        self.url = None  # set by calling the below method

    def resolve_repository_uri(self, url=None):
        if not self.url or url:
            # TODO: Should be a logging or debugging statement
            PRINT('NOTE: Calling out to ECR')
            try:
                resp = self.ecr_client.describe_repositories()
                for repo in resp.get('repositories', []):
                    if repo['repositoryUri'].endswith(self.env):
                        url = repo['repositoryUri']
            except Exception as e:
                # TODO: Should be a logging or debugging statement
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
            [auth_data] = self.ecr_client.get_authorization_token()['authorizationData']
            return auth_data
        except Exception as e:
            # TODO: Should be a logging or debugging statement
            PRINT('Could not acquire ECR authorization credentials: %s' % e)
            raise

    @staticmethod
    def extract_ecr_password_from_authorization(*, authorization):
        """ Extracts the password from the authorization token returned from the
            above API call.
        """
        return base64.b64decode(authorization['authorizationToken']).replace(b'AWS:', b'').decode('utf-8')

    def get_images_descriptions(self,
                                digests: Optional[List[str]] = None,
                                tags: Optional[List[str]] = None,
                                limit: Optional[Union[int, str]] = IMAGE_LIST_DEFAULT_COUNT_LIMIT):
        """
        Args:
            digests: a list of image digests (each represented as a string in sha256 format)
            tags: a list of strings (each being a potential or actual image tag).
            limit: an int indicating a limit on how many results to return,
                   a string indicating a limiting tag,
                   or None indicating no limit.

        Returns:
            a dictionary with keys 'descriptions' (the primary result), 'count' (the number of descriptions),
            and 'total' (the total number of registered images in the catalog).
            The descriptions are a list of individual dictionaries with keys that include, at least,
            'imagePushedAt', 'imageTags', and 'imageDigest'.
        """
        next_token = None
        image_descriptions = []
        # We don't know what order they are in, so we need to pull them all down,
        # and only hen sort them before applying the limit.
        while True:
            options = {'repositoryName': self.ecs_repository}
            if next_token:
                options['nextToken'] = next_token
            else:
                ids = []
                if digests:
                    # We may only provide this option on the first call, and only if it's non-null.
                    ids.extend([{'imageDigest': digest} for digest in digests])
                if tags:
                    ids.extend([{'imageTag': tag} for tag in tags])
                if ids:
                    options['imageIds'] = ids
                else:
                    options['maxResults'] = self.IMAGE_LIST_CHUNK_SIZE  # can only be provided on the first call
            response = self.ecr_client.describe_images(**options)
            image_descriptions.extend(response['imageDetails'])
            next_token = response.get('nextToken')
            if not next_token:
                break
        image_descriptions = sorted(image_descriptions, key=lambda x: x['imagePushedAt'], reverse=True)
        total = len(image_descriptions)
        image_descriptions = self._apply_image_descriptions_limit(image_descriptions=image_descriptions, limit=limit)
        return {'descriptions': image_descriptions, 'count': len(image_descriptions), 'total': total}

    @classmethod
    def _apply_image_descriptions_limit(cls, image_descriptions, limit):
        if not limit:
            return image_descriptions
        elif isinstance(limit, int):
            return image_descriptions[:limit]
        elif isinstance(limit, str):
            new_image_descriptions = []
            for image_description in image_descriptions:
                new_image_descriptions.append(image_description)
                if limit in image_description.get('imageTags', []):
                    break
            return new_image_descriptions
        else:
            raise ValueError("A limit must be an integer position, a string tag, or None.")



# These two variables are deprecated. Please use references to ECRUtils instead. They will go away in the future.
# CGAP_ECR_LAYOUT = ECRUtils.ECR_LAYOUT
CGAP_ECR_REGION = ECRUtils.REGION
