from __future__ import print_function

import boto3
import botocore.client
import contextlib
import json
import logging
import mimetypes
import os
import urllib.request

from io import BytesIO
from typing import Optional
from zipfile import ZipFile
from .base import get_beanstalk_real_url
from .env_utils import is_stg_or_prd_env, prod_bucket_env, full_env_name
from .exceptions import (
    InferredBucketConflict, CannotInferEnvFromNoGlobalEnvs, CannotInferEnvFromManyGlobalEnvs, MissingGlobalEnv,
    GlobalBucketAccessError, SynonymousEnvironmentVariablesMismatched,
)
from .misc_utils import PRINT, override_environ, ignored, exported


exported(get_beanstalk_real_url)


###########################
# Config
###########################
logging.basicConfig()
logger = logging.getLogger(__name__)


class HealthPageKey:  # This is moving here from cgap-portal.
    APPLICATION_BUCKET_PREFIX = 'application_bucket_prefix'
    BEANSTALK_APP_VERSION = 'beanstalk_app_version'
    BEANSTALK_ENV = 'beanstalk_env'
    BLOB_BUCKET = 'blob_bucket'                              # = s3Utils.BLOB_BUCKET_HEALTH_PAGE_KEY
    DATABASE = 'database'
    DISPLAY_TITLE = 'display_title'
    ELASTICSEARCH = 'elasticsearch'
    FILE_UPLOAD_BUCKET = 'file_upload_bucket'                # = s3Utils.RAW_BUCKET_HEALTH_PAGE_KEY
    FOURSIGHT = 'foursight'
    FOURSIGHT_BUCKET_PREFIX = 'foursight_bucket_prefix'
    IDENTITY = 'identity'
    INDEXER = 'indexer'
    INDEX_SERVER = 'index_server'
    LOAD_DATA = 'load_data'
    METADATA_BUNDLES_BUCKET = 'metadata_bundles_bucket'      # = s3Utils.METADATA_BUCKET_HEALTH_PAGE_KEY
    NAMESPACE = 'namespace'
    PROCESSED_FILE_BUCKET = 'processed_file_bucket'          # = s3Utils.OUTFILE_BUCKET_HEALTH_PAGE_KEY
    PROJECT_VERSION = 'project_version'
    S3_ENCRYPT_KEY_ID = 's3_encrypt_key_id'
    SNOVAULT_VERSION = 'snovault_version'
    SYSTEM_BUCKET = 'system_bucket'                          # = s3Utils.SYS_BUCKET_HEALTH_PAGE_KEY
    TIBANNA_CWLS_BUCKET = 'tibanna_cwls_bucket'              # = s3Utils.TIBANNA_CWLS_BUCKET_HEALTH_PAGE_KEY
    TIBANNA_OUTPUT_BUCKET = 'tibanna_output_bucket'          # = s3Utils.TIBANNA_OUTPUT_BUCKET_HEALTH_PAGE_KEY
    UPTIME = 'uptime'
    UTILS_VERSION = 'utils_version'


class s3Utils(object):  # NOQA - This class name violates style rules, but a lot of things might break if we change it.

    # Some extra variables used in setup here so that other modules can be consistent with chosen values.

    SYS_BUCKET_SUFFIX = "system"
    OUTFILE_BUCKET_SUFFIX = "wfoutput"
    RAW_BUCKET_SUFFIX = "files"
    BLOB_BUCKET_SUFFIX = "blobs"
    METADATA_BUCKET_SUFFIX = "metadata-bundles"
    TIBANNA_OUTPUT_BUCKET_SUFFIX = 'tibanna-output'
    TIBANNA_CWLS_BUCKET_SUFFIX = 'tibanna-cwls'

    s3_encrypt_key_id = None  # default. might be overridden based on health page in various places below

    EB_PREFIX = "elasticbeanstalk"
    EB_AND_ENV_PREFIX = EB_PREFIX + "-%s-"  # = "elasticbeanstalk-%s-"

    SYS_BUCKET_TEMPLATE = EB_AND_ENV_PREFIX + SYS_BUCKET_SUFFIX            # = "elasticbeanstalk-%s-system"
    OUTFILE_BUCKET_TEMPLATE = EB_AND_ENV_PREFIX + OUTFILE_BUCKET_SUFFIX    # = "elasticbeanstalk-%s-wfoutput"
    RAW_BUCKET_TEMPLATE = EB_AND_ENV_PREFIX + RAW_BUCKET_SUFFIX            # = "elasticbeanstalk-%s-files"
    BLOB_BUCKET_TEMPLATE = EB_AND_ENV_PREFIX + BLOB_BUCKET_SUFFIX          # = "elasticbeanstalk-%s-blobs"
    METADATA_BUCKET_TEMPLATE = EB_AND_ENV_PREFIX + METADATA_BUCKET_SUFFIX  # = "elasticbeanstalk-%s-metadata-bundles"
    TIBANNA_OUTPUT_BUCKET_TEMPLATE = TIBANNA_OUTPUT_BUCKET_SUFFIX          # = "tibanna-output" (no prefix)
    TIBANNA_CWLS_BUCKET_TEMPLATE = TIBANNA_CWLS_BUCKET_SUFFIX              # = "tibanna-cwls" (no prefix)

    # NOTE: These were deprecated and retained for compatibility in dcicutils 2.
    #       For dcicutils 3.0, please rewrite uses as HealthPageKey.xyz names.
    # SYS_BUCKET_HEALTH_PAGE_KEY = HealthPageKey.SYSTEM_BUCKET                     # = 'system_bucket'
    # OUTFILE_BUCKET_HEALTH_PAGE_KEY = HealthPageKey.PROCESSED_FILE_BUCKET         # = 'processed_file_bucket'
    # RAW_BUCKET_HEALTH_PAGE_KEY = HealthPageKey.FILE_UPLOAD_BUCKET                # = 'file_upload_bucket'
    # BLOB_BUCKET_HEALTH_PAGE_KEY = HealthPageKey.BLOB_BUCKET                      # = 'blob_bucket'
    # METADATA_BUCKET_HEALTH_PAGE_KEY = HealthPageKey.METADATA_BUNDLES_BUCKET      # = 'metadata_bundles_bucket'
    # TIBANNA_CWLS_BUCKET_HEALTH_PAGE_KEY = HealthPageKey.TIBANNA_CWLS_BUCKET      # = 'tibanna_cwls_bucket'
    # TIBANNA_OUTPUT_BUCKET_HEALTH_PAGE_KEY = HealthPageKey.TIBANNA_OUTPUT_BUCKET  # = 'tibanna_output_bucket'
    # This is also deprecated, even though not a bucket name. Use HealthPageKey.ELASTICSEARCH.
    # ELASTICSEARCH_HEALTH_PAGE_KEY = HealthPageKey.ELASTICSEARCH                  # = 'elasticsearch'

    @staticmethod  # backward compatibility in case other repositories are using this
    def verify_and_get_env_config(s3_client, global_bucket: str, env):
        return EnvManager.verify_and_get_env_config(s3_client=s3_client,
                                                    global_bucket=global_bucket,
                                                    env=env)

    @staticmethod  # backward compatibility in case other repositories are using this
    def fetch_health_page_json(url, use_urllib=True):
        return EnvManager.fetch_health_page_json(url=url, use_urllib=use_urllib)

    def __init__(self, outfile_bucket=None, sys_bucket=None, raw_file_bucket=None,
                 blob_bucket=None, metadata_bucket=None, tibanna_output_bucket=None,
                 tibanna_cwls_bucket=None,
                 # The env arg is not allowed to be passed positionally because we periodically add preceding args.
                 *, env=None):
        """ Initializes s3 utils in one of three ways:
        1) If 'GLOBAL_ENV_BUCKET' is set to an S3 env bucket, use that bucket to fetch the env for the buckets.
           We then use this env to build the bucket names. If there is only one such env, env can be None or omitted.
        2) With GLOBAL_ENV_BUCKET not set, if we instead pass in the env kwarg,
           we use this kwarg to build the bucket names according to legacy conventions.
        3) With no GLOBAL_ENV_BUCKET or env kwarg,
           we expect bucket kwargs to be set, and use those as bucket names directly.
        """
        self.url = ''
        self.s3 = boto3.client('s3', region_name='us-east-1')
        global_bucket = EnvManager.global_env_bucket_name()
        self.env_manager = None  # In a legacy environment, this will continue to be None
        if sys_bucket is None:
            # The choice to discriminate first on sys_bucket being None is part of the resolution of
            # https://hms-dbmi.atlassian.net/browse/C4-674
            if global_bucket:
                # env_config = self.verify_and_get_env_config(s3_client=self.s3, global_bucket=global_bucket, env=env)
                # ff_url = env_config['fourfront']
                self.env_manager = global_manager = EnvManager(env_name=env, s3=self.s3)
                self.url = global_manager.portal_url
                health_json_url = f'{global_manager.portal_url}/health?format=json'
                logger.warning('health json url: {}'.format(health_json_url))
                health_json = EnvManager.fetch_health_page_json(url=health_json_url)
                self.s3_encrypt_key_id = health_json.get(HealthPageKey.S3_ENCRYPT_KEY_ID, None)
                sys_bucket_from_health_page = health_json[HealthPageKey.SYSTEM_BUCKET]
                outfile_bucket_from_health_page = health_json[HealthPageKey.PROCESSED_FILE_BUCKET]
                raw_file_bucket_from_health_page = health_json[HealthPageKey.FILE_UPLOAD_BUCKET]
                blob_bucket_from_health_page = health_json[HealthPageKey.BLOB_BUCKET]
                metadata_bucket_from_health_page = health_json.get(HealthPageKey.METADATA_BUNDLES_BUCKET,
                                                                   # N/A for 4DN
                                                                   None)
                tibanna_cwls_bucket_from_health_page = health_json.get(HealthPageKey.TIBANNA_CWLS_BUCKET,
                                                                       # new, so it may be missing
                                                                       None)
                tibanna_output_bucket_from_health_page = health_json.get(HealthPageKey.TIBANNA_OUTPUT_BUCKET,
                                                                         # new, so it may be missing
                                                                         None)
                sys_bucket = sys_bucket_from_health_page  # OK to overwrite because we checked it's None above
                if outfile_bucket and outfile_bucket != outfile_bucket_from_health_page:
                    raise InferredBucketConflict(kind="outfile", specified=outfile_bucket,
                                                 inferred=outfile_bucket_from_health_page)
                else:
                    outfile_bucket = outfile_bucket_from_health_page
                if raw_file_bucket and raw_file_bucket != raw_file_bucket_from_health_page:
                    raise InferredBucketConflict(kind="raw file", specified=raw_file_bucket,
                                                 inferred=raw_file_bucket_from_health_page)
                else:
                    raw_file_bucket = raw_file_bucket_from_health_page
                if blob_bucket and blob_bucket != blob_bucket_from_health_page:
                    raise InferredBucketConflict(kind="blob", specified=blob_bucket,
                                                 inferred=blob_bucket_from_health_page)
                else:
                    blob_bucket = blob_bucket_from_health_page
                if metadata_bucket and metadata_bucket != metadata_bucket_from_health_page:
                    raise InferredBucketConflict(kind="metadata", specified=metadata_bucket,
                                                 inferred=metadata_bucket_from_health_page)
                else:
                    metadata_bucket = metadata_bucket_from_health_page
                if tibanna_cwls_bucket and tibanna_cwls_bucket != tibanna_cwls_bucket_from_health_page:
                    raise InferredBucketConflict(kind="tibanna cwls", specified=tibanna_cwls_bucket,
                                                 inferred=tibanna_cwls_bucket_from_health_page)
                else:
                    tibanna_cwls_bucket = tibanna_cwls_bucket_from_health_page
                if tibanna_output_bucket and tibanna_output_bucket != tibanna_output_bucket_from_health_page:
                    raise InferredBucketConflict(kind="tibanna output", specified=tibanna_output_bucket,
                                                 inferred=tibanna_output_bucket_from_health_page)
                else:
                    tibanna_output_bucket = tibanna_output_bucket_from_health_page
                logger.warning('Buckets resolved successfully.')
            else:
                # staging and production share same buckets
                # TODO: As noted in some of the comments on this conditional, when the new env_utils with
                #       orchestration support is in place, this same generality needs to be done
                #       upstream of the global env bucket branch, too. That's not needed for orchestrated cgap,
                #       which has no stage, but it will be needed for orchestrated fourfront. -kmp 31-Aug-2021
                if env:
                    if is_stg_or_prd_env(env):
                        self.url = get_beanstalk_real_url(env)  # done BEFORE prod_bucket_env blurring stg/prd
                        env = prod_bucket_env(env)
                    else:
                        # TODO: This is the part that is not yet supported in env_utils, but there is a pending
                        #       patch that will fix that. -kmp 31-AUg-2021
                        env = full_env_name(env)
                        self.url = get_beanstalk_real_url(env)  # done AFTER maybe prepending cgap- or foursight-.

                    health_json_url = f"{self.url}/health?format=json"
                    # In the orchestrated case, we issue a warning here. Do we need that? -kmp 1-Sep-2021
                    # logger.warning('health json url: {}'.format(health_json_url))
                    health_json = EnvManager.fetch_health_page_json(url=health_json_url)
                    es_url = health_json.get(HealthPageKey.ELASTICSEARCH)
                    if not es_url.startswith("http"):  # will match http: and https:
                        es_url = f"https://{es_url}"
                    self.env_manager = EnvManager.compose(portal_url=self.url, es_url=es_url, env_name=env, s3=self.s3)
                    self.s3_encrypt_key_id = health_json.get(HealthPageKey.S3_ENCRYPT_KEY_ID, None)

                # TODO: This branch is not setting self.global_env_bucket_manager, but it _could_ do that from the
                #       description. -kmp 21-Aug-2021
                def apply_template(template, env):
                    return template % env if "%s" in template else template
                # we use standardized naming schema, so s3 buckets always have same prefix
                sys_bucket = apply_template(self.SYS_BUCKET_TEMPLATE, env)
                outfile_bucket = apply_template(self.OUTFILE_BUCKET_TEMPLATE, env)
                raw_file_bucket = apply_template(self.RAW_BUCKET_TEMPLATE, env)
                blob_bucket = apply_template(self.BLOB_BUCKET_TEMPLATE, env)
                metadata_bucket = apply_template(self.METADATA_BUCKET_TEMPLATE, env)
                tibanna_cwls_bucket = apply_template(self.TIBANNA_CWLS_BUCKET_TEMPLATE, env)
                tibanna_output_bucket = apply_template(self.TIBANNA_OUTPUT_BUCKET_TEMPLATE, env)
        else:
            # If at least sys_bucket was given, for legacy reasons (see https://hms-dbmi.atlassian.net/browse/C4-674)
            # we assume that the given buckets are exactly the ones we want and we don't set up any others.
            # It follows from this that if not all the buckets are given, some may end up being None, but we assume
            # those won't be needed. -kmp 23-Jun-2021
            pass

        self.sys_bucket = sys_bucket
        self.outfile_bucket = outfile_bucket
        self.raw_file_bucket = raw_file_bucket
        self.blob_bucket = blob_bucket
        self.metadata_bucket = metadata_bucket
        self.tibanna_cwls_bucket = tibanna_cwls_bucket
        self.tibanna_output_bucket = tibanna_output_bucket

    ACCESS_KEYS_S3_KEY = 'access_key_admin'

    def get_access_keys(self, name=ACCESS_KEYS_S3_KEY):
        keys = self.get_key(keyfile_name=name)
        if not isinstance(keys, dict):
            raise ValueError("Remotely stored access keys are not in the expected form")

        if isinstance(keys.get('default'), dict):
            keys = keys['default']
        if self.url:
            keys['server'] = self.url
        return keys

    def get_ff_key(self):
        return self.get_access_keys()

    def get_higlass_key(self):
        # higlass key corresponds to Django server super user credentials
        return self.get_key(keyfile_name='api_key_higlass')

    def get_google_key(self):
        return self.get_key(keyfile_name='api_key_google')

    def get_jupyterhub_key(self):
        # jupyterhub key is a Jupyterhub API token
        return self.get_key(keyfile_name='api_key_jupyterhub')

    def get_key(self, keyfile_name='access_key_admin'):
        # Share secret encrypted S3 File
        response = self.s3.get_object(Bucket=self.sys_bucket,
                                      Key=keyfile_name,
                                      SSECustomerKey=os.environ['S3_ENCRYPT_KEY'],
                                      SSECustomerAlgorithm='AES256')
        akey = response['Body'].read()
        if type(akey) == bytes:
            akey = akey.decode()
        try:
            return json.loads(akey)
        except (ValueError, TypeError):
            # maybe its not json after all
            return akey

    def read_s3(self, filename):
        response = self.s3.get_object(Bucket=self.outfile_bucket, Key=filename)
        logger.info(str(response))
        return response['Body'].read()

    def does_key_exist(self, key, bucket=None, print_error=True):
        if not bucket:
            bucket = self.outfile_bucket
        try:
            file_metadata = self.s3.head_object(Bucket=bucket, Key=key)
        except Exception as e:
            if print_error:
                PRINT("object %s not found on bucket %s" % (str(key), str(bucket)))
                PRINT(str(e))
            return False
        return file_metadata

    def get_file_size(self, key, bucket=None, add_bytes=0, add_gb=0,
                      size_in_gb=False):
        """
        default returns file size in bytes,
        unless size_in_gb = True
        """
        meta = self.does_key_exist(key, bucket)
        if not meta:
            raise Exception("key not found")
        one_gb = 1073741824
        add = add_bytes + (add_gb * one_gb)
        size = meta['ContentLength'] + add  # noQA - PyCharm type inferencing is wrong about fussing here
        if size_in_gb:
            size = size / one_gb
        return size

    def delete_key(self, key, bucket=None):
        if not bucket:
            bucket = self.outfile_bucket
        self.s3.delete_object(Bucket=bucket, Key=key)

    @classmethod
    def size(cls, bucket):
        sbuck = boto3.resource('s3').Bucket(bucket)
        # get only head of objects so we can count them
        return sum(1 for _ in sbuck.objects.all())

    def s3_put(self, obj, upload_key, acl=None):
        """
        try to guess content type
        """
        content_type = mimetypes.guess_type(upload_key)[0]
        if content_type is None:
            content_type = 'binary/octet-stream'
        if acl:
            # we use this to set some of the object as public
            return self.s3.put_object(Bucket=self.outfile_bucket,
                                      Key=upload_key,
                                      Body=obj,
                                      ContentType=content_type,
                                      ACL=acl)
        else:
            return self.s3.put_object(Bucket=self.outfile_bucket,
                                      Key=upload_key,
                                      Body=obj,
                                      ContentType=content_type)

    def s3_put_secret(self, data, keyname, bucket=None, secret=None):
        if not bucket:
            bucket = self.sys_bucket
        if not secret:
            secret = os.environ["S3_ENCRYPT_KEY"]
        return self.s3.put_object(Bucket=bucket,
                                  Key=keyname,
                                  Body=data,
                                  SSECustomerKey=secret,
                                  SSECustomerAlgorithm='AES256')

    def s3_read_dir(self, prefix):
        return self.s3.list_objects(Bucket=self.outfile_bucket, Prefix=prefix)

    def s3_delete_dir(self, prefix):
        # one query get list of all the files we want to delete
        obj_list = self.s3.list_objects(Bucket=self.outfile_bucket, Prefix=prefix)
        files = obj_list.get('Contents', [])

        # morph file list into format that boto3 wants
        delete_keys = {'Objects': [{'Key': k}
                                   for k in [obj['Key']
                                             for obj in files]]}

        # second query deletes all the files, NOTE: Max 1000 files
        if delete_keys['Objects']:
            self.s3.delete_objects(Bucket=self.outfile_bucket, Delete=delete_keys)

    def read_s3_zipfile(self, s3key, files_to_extract):
        s3_stream = self.read_s3(s3key)
        bytestream = BytesIO(s3_stream)
        zipstream = ZipFile(bytestream, 'r')
        ret_files = {}

        for name in files_to_extract:
            # search subdirectories for file with name
            # so I don't have to worry about figuring out the subdirs
            zipped_filename = find_file(name, zipstream)
            if zipped_filename:
                ret_files[name] = zipstream.open(zipped_filename).read()
        return ret_files

    def unzip_s3_to_s3(self, zipped_s3key, dest_dir, acl=None, store_results=True):
        """stream the content of a zipped key on S3 to another location on S3.
        if store_results=True, it saves the content and returns it in the dictionary format
        (default)
        """

        if not dest_dir.endswith('/'):
            dest_dir += '/'

        s3_stream = self.read_s3(zipped_s3key)
        # read this badboy to memory, don't go to disk
        bytestream = BytesIO(s3_stream)
        zipstream = ZipFile(bytestream, 'r')

        # The contents of zip can sometimes be like
        # ["foo/", "file1", "file2", "file3"]
        # and other times like
        # ["file1", "file2", "file3"]
        file_list = zipstream.namelist()
        if file_list[0].endswith('/'):
            # in case directory first name in the list
            basedir_name = file_list.pop(0)
        else:
            basedir_name = ''

        ret_files = {}
        for file_name in file_list:
            # don't copy dirs just files
            if not file_name.endswith('/'):
                if basedir_name:
                    s3_file_name = file_name.replace(basedir_name, dest_dir)
                else:
                    s3_file_name = dest_dir + file_name
                s3_key = "https://s3.amazonaws.com/%s/%s" % (self.outfile_bucket, s3_file_name)
                # just perf optimization so we don't have to copy
                # files twice that we want to further interrogate
                the_file = zipstream.open(file_name, 'r').read()
                file_to_find = os.path.basename(file_name)
                if store_results:
                    ret_files[file_to_find] = {'s3key': s3_key,
                                               'data': the_file}
                self.s3_put(the_file, s3_file_name, acl=acl)

        return ret_files


class EnvManager:

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
            elif env_name != described_env_name:
                raise ValueError(f"The given env name, {env_name},"
                                 f" does not match the name given in the description, {env_description}.")

        self._env_name = env_name
        if not self._env_name:
            raise ValueError(f"Missing {self.LEGACY_ENV_NAME_KEY!r} or {self.ENV_NAME_KEY!r}"
                             f" key in global_env {env_description}.")

        self._portal_url = (env_description.get(self.LEGACY_PORTAL_URL_KEY) or
                            env_description.get(self.PORTAL_URL_KEY))
        if not self._portal_url:
            raise ValueError(f"Missing {self.LEGACY_PORTAL_URL_KEY!r} or {self.PORTAL_URL_KEY!r}"
                             f" key in global_env {env_description}.")

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

    @staticmethod
    def verify_and_get_env_config(s3_client, global_bucket: str, env):
        """
        Verifies the S3 environment from which the env config is coming from, and returns the S3-based env config
        Throws exceptions if the S3 bucket is unreachable, or an env based on the name of the global S3 bucket
        is not present.
        """
        logger.warning(f'Fetching bucket data via global env bucket: {global_bucket}')
        head_response = s3_client.head_bucket(Bucket=global_bucket)
        status = head_response['ResponseMetadata']['HTTPStatusCode']  # should be 200; raise error for 404 or 403
        if status != 200:
            raise GlobalBucketAccessError(global_bucket=global_bucket, status=status)
        # list contents of global env bucket, look for a match with the global env bucket name
        list_response = s3_client.list_objects_v2(Bucket=global_bucket)
        # no match, raise exception
        if list_response['KeyCount'] < 1:
            raise CannotInferEnvFromNoGlobalEnvs(global_bucket=global_bucket)
        keys = [content['Key'] for content in list_response['Contents']]
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

    @classmethod
    def global_env_bucket_name(cls):
        """
        This class method returns the name of the current 'global env bucket', the bucket where meanings of
        environment names are looked up in orchestrated environments.
        """
        global_bucket_env_var = 'GLOBAL_BUCKET_ENV'  # Deprecated. Supported for now since some tools started using it.
        global_env_bucket_var = 'GLOBAL_ENV_BUCKET'  # Preferred name. Please transition code to use this.
        global_bucket_env = os.environ.get(global_bucket_env_var)
        global_env_bucket = os.environ.get(global_env_bucket_var)
        if global_env_bucket and global_bucket_env and global_env_bucket != global_bucket_env:
            raise SynonymousEnvironmentVariablesMismatched(var1=global_bucket_env_var, val1=global_bucket_env,
                                                           var2=global_env_bucket_var, val2=global_env_bucket)
        global_bucket = global_env_bucket or global_bucket_env
        return global_bucket

    @classmethod
    @contextlib.contextmanager
    def global_env_bucket_named(cls, name):
        """
        This class method, a 'context manager' useful with the Python 'with' operation, binds the name of
        the current 'global env bucket', the bucket where meanings of environment names are looked up
        in orchestrated environments.
        """

        with override_environ(GLOBAL_BUCKET_ENV=name, GLOBAL_ENV_BUCKET=name):
            yield


def find_file(name, zipstream):
    for zipped_filename in zipstream.namelist():
        if zipped_filename.endswith(name):
            return zipped_filename
