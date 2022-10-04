import boto3
import base64
import json
import logging
import os

from botocore.client import BaseClient
from botocore.exceptions import ClientError
from typing import List, Optional, Tuple, Union
from .common import REGION as COMMON_REGION
from .misc_utils import get_error_message


logger = logging.getLogger(__name__)


class ECRUtils:
    """ Utility class for interacting with ECR.
        Initialized with an env name, from which a repo URL is resolved.

        NOTE 1: unlike s3Utils, the resolved resources (repository) will NOT be
        created if it does not already exist.
        NOTE 2: the (already created) ECR repository must have the env_name in the URI
    """

    # This tag is presently called "latest", but I'd like to call it "released". -kmp 12-Mar-2022
    # For now refer to it indirectly through a variable.

    IMAGE_RELEASED_TAG = 'latest'

    # In many cases, the ECR repo named 'main' is where images live.
    # There could be blue/green deploys or other multi-environment account situations where others are used.
    DEFAULT_IMAGE_REPOSITORY = 'main'

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
    def __init__(self, *, env_name=None, local_repository=None, region=None, ecr_client=None, image_repository=None):
        """
        Creates an ECR client on startup/

        :param env_name: the name of a CGAP or Fourfront portal environment
        :param local_repository: deprecated, unused argument
        :param region: the AWS region to use if an ECR client needs to be created
        :param ecr_client: an AWS boto3 ECR client if one is already created with appropriae arguments
        :param image_repository: the name of an ECR image repository to use.
        """
        self.env = env_name or os.environ.get('ENV_NAME')
        self.local_repository = local_repository  # Not sure this is even used any more. Should we deprecate it?
        self.ecr_client = ecr_client or boto3.client('ecr', region_name=region or self.REGION)
        self.image_repository = image_repository or self.DEFAULT_IMAGE_REPOSITORY
        self.url = None  # set by calling the below method

    def resolve_repository_uri(self, url=None):
        if not self.url or url:
            logger.info(f"Calling ECR.resolve_repository_uri, url={url!r}")
            try:
                resp = self.ecr_client.describe_repositories()
                for repo in resp.get('repositories', []):
                    if repo['repositoryUri'].endswith(self.env):
                        url = repo['repositoryUri']
            except Exception as e:
                logger.error(f"Could not retrieve repository information from ECR. {get_error_message(e)}")
                pass
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
            logger.error(f"Could not acquire ECR authorization credentials. {get_error_message(e)}")
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
        # and only then sort them before applying the limit.
        while True:
            options = {'repositoryName': self.image_repository}
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
        """
        Internal helper for get_images_descriptions to apply certain filters
        """
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


class ECRTagWatcher:

    DEFAULT_TAG = ECRUtils.IMAGE_RELEASED_TAG
    DEFAULT_REGION = ECRUtils.REGION

    def __init__(self, *, tag: Optional[str] = None, region: str = None, image_repository: Optional[str] = None,
                 ecr_client: Optional[BaseClient] = None, ecr_utils: Optional[ECRUtils] = None):
        """

        :param tag: a tag to watch for, which if None or unsupplied will default
            from the class variable DEFAULT_TAG,
            which is in turn defaulted from ECRUtils.IMAGE_RELEASED_TAG)
        :param region: an AWS region, which if None or unsupplied will default
            from the class variable DEFAULT_REGION,
            which is in turn defaulted from ECRUtils.REGION
        :param image_repository: an ECR repository name, which is None or unsupplied
            will be defaulted by ECRUtils using ECRUtils.DEFAULT_IMAGE_REPOSITORY.
        :param ecr_client: an AWS ecr client, in case you have one already.
            One will be created if you don't.
        :param ecr_utils: an ECRUtils object, in case you already have one that has been initialized appropriately.
            One will be created if you don't.
        """
        self.tag = tag or self.DEFAULT_TAG
        self.ecr_utils = ecr_utils or ECRUtils(image_repository=image_repository,
                                               ecr_client=(ecr_client
                                                           or boto3.client('ecr',
                                                                           region_name=(region
                                                                                        or self.DEFAULT_REGION))))
        self.last_image_digest = self.get_current_image_digest()

    def get_current_image_digest(self) -> Optional[str]:
        """
        Returns the image digest of image in the watcher's ECS repository that is currently tagged with the watched tag,
        or None if there is no such tagged image

        Returns:
            an image digest or None
        """
        try:
            summary = self.ecr_utils.get_images_descriptions(tags=[self.tag])
            if summary:
                descriptions = summary['descriptions']
                if len(descriptions) != 1:
                    raise ValueError(f"Expected exactly one tagged image. Got {json.dumps(descriptions, default=str)}")
                return descriptions[0]['imageDigest']
            return None
        except ClientError as e:
            if e.response['Error']['Code'] == 'ImageNotFoundException':
                return None
            else:
                raise

    def check_if_image_digest_changed(self) -> Tuple[bool, Optional[str]]:
        """
        Checks the watcher's ECS respository for the image tag associated with watcher,
        returning two pieces of information:

        * whether the image has changed since the last time a check was done
        * the image digest for the currently tagged image (or None if there is no image currently tagged)

        Use this method if you need to distinguish between 'changed to None' and 'did not change'.
        Neither of these is a situation where there is a new image to deploy. If all you want
        to know is whether there is a reason to deploy due to change, use check_for_new_image_to_deploy.

        Returns:
            a tuple of (changed, current_digest) where changed is a boolean saying whether
            the value changed since last time, and current_digest is the digest description
            of the current image with that tag.
        """
        current_digest = self.get_current_image_digest()
        changed = self.last_image_digest != current_digest
        self.last_image_digest = current_digest
        return changed, current_digest

    def check_for_new_image_to_deploy(self) -> Optional[str]:
        """
        Checks the watcher's ECS respository for the image tag associated with watcher,
        returning the image digest of an image description to deploy or None.

        Use this method if you just want to know if now is a good time to start a deploy.

        Note that this does not distinguish the case of tagging state being unchanged
        and a change to a state in which there is no image associated with the tag.
        Neither of these cases would be a good time to deploy.
        Only when an actual newly-tagged image is available will this return a true value (the new image digest).

        Returns:
            the image digest of an image to be deployed if a new one has been newly tagged
            since the last check, or None otherwise
        """
        changed, new_digest = self.check_if_image_digest_changed()
        return new_digest if changed else None


# These two variables are deprecated. Please use references to ECRUtils instead. They will go away in the future.
# CGAP_ECR_LAYOUT = ECRUtils.ECR_LAYOUT
CGAP_ECR_REGION = ECRUtils.REGION
