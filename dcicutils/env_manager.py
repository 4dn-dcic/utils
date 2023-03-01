import boto3
import botocore.client
import json
import logging
import urllib.request

from typing import Optional
from .env_base import EnvBase
from .env_utils import full_env_name
from .exceptions import CannotInferEnvFromManyGlobalEnvs, MissingGlobalEnv
from .misc_utils import ignored


logger = logging.getLogger(__name__)


class EnvManager(EnvBase):

    # I have built in an upgrade strategy where these can have some better keys, but for now all code should be
    # converted to do the following:
    #
    # If you are creating an env, add BOTH keys, so we can phase out the old key campatibly at some point in
    # the future. If you are accessing ane of these descriptions, do so by using this abstraction to create
    # an EnvManager instance, then access the quantities via the .portal_url, .es_url, or .env_name attributes.
    # Then you won't have to know whether it came from the old or new key.

    LEGACY_PORTAL_URL_KEY = 'fourfront'
    PORTAL_URL_KEY = 'portal_url'  # In the future we may want to convert to these keys, but not yet. See note above.

    LEGACY_ES_URL_KEY = 'es'
    ES_URL_KEY = 'es_url'  # In the future we may want to convert to these keys, but not yet. See note above.

    LEGACY_ENV_NAME_KEY = 'ff_env'
    ENV_NAME_KEY = 'env_name'  # In the future we may want to convert to these keys, but not yet. See note above.

    @classmethod
    def compose(cls, *, portal_url, es_url, env_name, s3: Optional[botocore.client.BaseClient] = None):
        """
        Creates an EnvManager from a set of required function arguments that together comprise an env_description
        that would be needed to create an EnvManager with an env_description argument. The s3 argument is not part
        of that description, but is still needed if available. In other words:

            EnvManager.compose(s3=s3, portal_url=portal_url, es_url=es_url, env_name=env_name)
            ==
            EnvManager(s3=s3, env_description={'fourfront': portal_url, 'es': es_url, 'ff_env': env_name})
        """
        # TODO: At some future time, use the non-LEGACY versions of keys.
        description = {
            EnvManager.LEGACY_PORTAL_URL_KEY: portal_url,
            EnvManager.LEGACY_ES_URL_KEY: es_url,
            EnvManager.LEGACY_ENV_NAME_KEY: env_name,
        }
        return cls(env_description=description, s3=s3)

    def __init__(self, env_name: Optional[str] = None,
                 env_description: Optional[dict] = None,
                 s3: Optional[botocore.client.BaseClient] = None):  # really we want an S3 client, but it's not a type
        """
        Creates an object that can manage various details of the current Fourfront or CGAP environment,
        such as 'fourfront-mastertest', 'data', etc.

        :param env_name: an environment name (optional, but preferred)
        :param env_description: a dictionary (optional, not preferred, a substitute for a name in testing or debugging)
        :param s3: an s3 client such as from boto3.client('s3') or from the .s3 attribute of an s3Utils instance

        Although the s3 client can be created for you, but if you already have acess to an s3 client via some existing
        object, you should pass that.
        """

        self.s3 = s3 or boto3.client('s3')
        if env_name and env_description:
            raise ValueError("You may only specify an env_name or an env_description")
        if env_description:
            self.env_description = env_description
        else:  # env_name is given or is None and must be inferred
            env_description = self.verify_and_get_env_config(s3_client=self.s3,
                                                             global_bucket=self.global_env_bucket_name(),
                                                             env=env_name)
            self.env_description = env_description

        described_env_name = (env_description.get(self.LEGACY_ENV_NAME_KEY) or
                              env_description.get(self.ENV_NAME_KEY))

        if described_env_name:
            if not env_name:
                env_name = described_env_name
            # TODO: Enable this test when entries for mastertest don't expand to "fourfront-mastertest", etc.
            # elif env_name != described_env_name:
            #     raise ValueError(f"The given env name, {env_name},"
            #                      f" does not match the name given in the description, {env_description}.")

        self._env_name = env_name
        if not self._env_name:
            raise ValueError(f"Missing {self.LEGACY_ENV_NAME_KEY!r} or {self.ENV_NAME_KEY!r}"
                             f" key in global_env {env_description}.")

        self._portal_url = (env_description.get(self.LEGACY_PORTAL_URL_KEY) or
                            env_description.get(self.PORTAL_URL_KEY))
        if not self._portal_url:
            raise ValueError(f"Missing {self.LEGACY_PORTAL_URL_KEY!r} or {self.PORTAL_URL_KEY!r}"
                             f" key in global_env {env_description}.")
        else:
            self._portal_url = self._portal_url.rstrip('/')

        self._es_url = (env_description.get(self.LEGACY_ES_URL_KEY) or
                        env_description.get(self.ES_URL_KEY))
        if not self._es_url:
            raise ValueError(f"Missing {self.LEGACY_ES_URL_KEY!r} or {self.ES_URL_KEY!r}"
                             f" key in global_env {env_description}.")

    @property
    def portal_url(self):
        """
        For the environment represented by self, this returns the portal URL, which for now is found
        by env_description.get('fourfront'), where env_description is the dictionary that was either
        given as an argument in creating the EnvManager instance or was looked up from the
        environment's description file in the global env bucket.

        In the future, we'll get it from the 'portal_url' property instead.
        """
        return self._portal_url

    @property
    def es_url(self):
        """
        For the environment represented by self, this returns the es URL, which for now is found
        by env_description.get('es'), where env_description is the dictionary that was either
        given as an argument in creating the EnvManager instance or was looked up from the
        environment's description file in the global env bucket.

        In the future, we'll get it from the 'es_url' property instead.
        """
        return self._es_url

    @property
    def env_name(self):
        """
        For the environment represented by self, this returns the es URL, which for now is found
        by env_description.get('ff_env'), where env_description is the dictionary that was either
        given as an argument in creating the EnvManager instance or was looked up from the
        environment's description file in the global env bucket.

        In the future, we'll get it from the 'env_name' property instead.
        """
        return self._env_name

    @classmethod
    def verify_and_get_env_config(cls, s3_client, global_bucket: str, env: Optional[str]):
        """
        Verifies the S3 environment from which the env config is coming from, and returns the S3-based env config
        Throws exceptions if the S3 bucket is unreachable, or an env based on the name of the global S3 bucket
        is not present.
        """
        logger.warning(f'Fetching bucket data via global env bucket: {global_bucket}')

        if env:
            env = full_env_name(env)

        # head_response = s3_client.head_bucket(Bucket=global_bucket)
        # status = head_response['ResponseMetadata']['HTTPStatusCode']  # should be 200; raise error for 404 or 403
        # if status != 200:
        #     raise GlobalBucketAccessError(global_bucket=global_bucket, status=status)
        # # list contents of global env bucket, look for a match with the global env bucket name
        # list_response = s3_client.list_objects_v2(Bucket=global_bucket)
        # # no match, raise exception
        # if list_response['KeyCount'] < 1:
        #     raise CannotInferEnvFromNoGlobalEnvs(global_bucket=global_bucket)
        # keys = [content['Key'] for content in list_response['Contents']]
        keys = cls.get_all_environments(env_bucket=global_bucket)

        if env is None:
            if len(keys) == 1:
                # If there is only one env, which is the likely case, let's infer that this is the one we want.
                env = keys[0]
                logger.warning(f"No env was specified, but {env} is the only one available, so using that.")
            else:
                raise CannotInferEnvFromManyGlobalEnvs(global_bucket=global_bucket, keys=keys)
        if env not in keys:
            raise MissingGlobalEnv(global_bucket=global_bucket, keys=keys, env=env)
        # we found a match, so fetch that file as config
        get_response = s3_client.get_object(Bucket=global_bucket, Key=env)
        env_config = json.loads(get_response['Body'].read())
        return env_config

    @staticmethod
    def fetch_health_page_json(url, use_urllib=True):
        # Eric&Will found requests.get(url).json() sometimes failed to made this alternative
        # based on urllib that we now use exclusively.
        ignored(use_urllib)
        res = urllib.request.urlopen(url)
        res_body = res.read()
        j = json.loads(res_body.decode("utf-8"))
        return j
