from __future__ import print_function

import boto3
import json
import logging
import mimetypes
import os
import requests
import urllib.request

from io import BytesIO
from zipfile import ZipFile
from .env_utils import is_stg_or_prd_env, prod_bucket_env, full_env_name
from .exceptions import (
    InferredBucketConflict, CannotInferEnvFromNoGlobalEnvs, CannotInferEnvFromManyGlobalEnvs, MissingGlobalEnv,
    GlobalBucketAccessError, SynonymousEnvironmentVariablesMismatched,
)
from .misc_utils import PRINT


###########################
# Config
###########################
logging.basicConfig()
logger = logging.getLogger(__name__)


class s3Utils(object):  # NOQA - This class name violates style rules, but a lot of things might break if we change it.

    # Some extra variables used in setup here so that other modules can be consistent with chosen values.

    SYS_BUCKET_SUFFIX = "system"
    OUTFILE_BUCKET_SUFFIX = "wfoutput"
    RAW_BUCKET_SUFFIX = "files"
    BLOB_BUCKET_SUFFIX = "blobs"
    METADATA_BUCKET_SUFFIX = "metadata-bundles"
    TIBANNA_OUTPUT_BUCKET_SUFFIX = 'tibanna-output'

    EB_PREFIX = "elasticbeanstalk"
    EB_AND_ENV_PREFIX = EB_PREFIX + "-%s-"  # = "elasticbeanstalk-%s-"

    SYS_BUCKET_TEMPLATE = EB_AND_ENV_PREFIX + SYS_BUCKET_SUFFIX            # = "elasticbeanstalk-%s-system"
    OUTFILE_BUCKET_TEMPLATE = EB_AND_ENV_PREFIX + OUTFILE_BUCKET_SUFFIX    # = "elasticbeanstalk-%s-wfoutput"
    RAW_BUCKET_TEMPLATE = EB_AND_ENV_PREFIX + RAW_BUCKET_SUFFIX            # = "elasticbeanstalk-%s-files"
    BLOB_BUCKET_TEMPLATE = EB_AND_ENV_PREFIX + BLOB_BUCKET_SUFFIX          # = "elasticbeanstalk-%s-blobs"
    METADATA_BUCKET_TEMPLATE = EB_AND_ENV_PREFIX + METADATA_BUCKET_SUFFIX  # = "elasticbeanstalk-%s-metadata-bundles"
    TIBANNA_OUTPUT_BUCKET_TEMPLATE = TIBANNA_OUTPUT_BUCKET_SUFFIX          # = "tibanna-output" (no prefix)

    SYS_BUCKET_HEALTH_PAGE_KEY = 'system_bucket'
    OUTFILE_BUCKET_HEALTH_PAGE_KEY = 'processed_file_bucket'
    RAW_BUCKET_HEALTH_PAGE_KEY = 'file_upload_bucket'
    BLOB_BUCKET_HEALTH_PAGE_KEY = 'blob_bucket'
    METADATA_BUCKET_HEALTH_PAGE_KEY = 'metadata_bundles_bucket'
    TIBANNA_OUTPUT_BUCKET_HEALTH_PAGE_KEY = 'tibanna_output_bucket'

    @staticmethod  # backwawrd compatibility in case other repositories are using this
    def verify_and_get_env_config(s3_client, global_bucket: str, env):
        """ Verifies the S3 environment from which the env config is coming from, and returns the S3-based env config
            Throws exceptions if the S3 bucket is unreachable, or an env based on the name of the global S3 bucket
            is not present.
        """
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
                logger.warning("No env was specified, but {env} is the only one available, so using that."
                               .format(env=env))
            else:
                raise CannotInferEnvFromManyGlobalEnvs(global_bucket=global_bucket, keys=keys)
        config_filename = None
        for filename in keys:
            if filename == env:
                config_filename = filename
        if not config_filename:
            raise MissingGlobalEnv(global_bucket=global_bucket, keys=keys, env=env)
        else:
            # we found a match, so fetch that file as config
            get_response = s3_client.get_object(Bucket=global_bucket, Key=config_filename)
            env_config = json.loads(get_response['Body'].read())
            return env_config

    @staticmethod
    def fetch_health_page_json(url, use_urllib):
        if use_urllib is False:
            return requests.get(url).json()
        else:
            res = urllib.request.urlopen(url)
            res_body = res.read()
            j = json.loads(res_body.decode("utf-8"))
            return j

    def __init__(self, outfile_bucket=None, sys_bucket=None, raw_file_bucket=None,
                 blob_bucket=None, metadata_bucket=None, tibanna_output_bucket=None, env=None):
        """ Initializes s3 utils in one of three ways:
        1) If 'GLOBAL_ENV_BUCKET' is set to an S3 env bucket, use that bucket to fetch the env for the buckets.
           We then use this env to build the bucket names. If there is only one such env, env can be None or omitted.
        2) With GLOBAL_ENV_BUCKET not set, if we instead pass in the env kwarg,
           we use this kwarg to build the bucket names according to legacy conventions.
        3) With no GLOBAL_ENV_BUCKET or env kwarg,
           we expect bucket kwargs to be set, and use those as bucket names directly.
        """
        # avoid circular ref
        from .beanstalk_utils import get_beanstalk_real_url
        self.url = ''
        self.s3 = boto3.client('s3', region_name='us-east-1')
        global_bucket_env_var = 'GLOBAL_BUCKET_ENV'  # Deprecated. Supported for now since some tools started using it.
        global_env_bucket_var = 'GLOBAL_ENV_BUCKET'  # Preferred name. Please transition code to use this.
        global_bucket_env = os.environ.get(global_bucket_env_var)
        global_env_bucket = os.environ.get(global_env_bucket_var)
        if global_env_bucket and global_bucket_env and global_env_bucket != global_bucket_env:
            raise SynonymousEnvironmentVariablesMismatched(var1=global_bucket_env_var, val1=global_bucket_env,
                                                           var2=global_env_bucket_var, val2=global_env_bucket)
        global_bucket = global_env_bucket or global_bucket_env
        if sys_bucket is None:
            # The choice to discriminate first on sys_bucket being None is part of the resolution of
            # https://hms-dbmi.atlassian.net/browse/C4-674
            if global_bucket:
                logger.warning('Fetching bucket data via global env bucket: {}'.format(global_bucket))
                env_config = self.verify_and_get_env_config(s3_client=self.s3, global_bucket=global_bucket, env=env)
                ff_url = env_config['fourfront']
                health_json_url = '{ff_url}/health?format=json'.format(ff_url=ff_url)
                logger.warning('health json url: {}'.format(health_json_url))
                health_json = self.fetch_health_page_json(url=health_json_url, use_urllib=True)
                sys_bucket_from_health_page = health_json[self.SYS_BUCKET_HEALTH_PAGE_KEY]
                outfile_bucket_from_health_page = health_json[self.OUTFILE_BUCKET_HEALTH_PAGE_KEY]
                raw_file_bucket_from_health_page = health_json[self.RAW_BUCKET_HEALTH_PAGE_KEY]
                blob_bucket_from_health_page = health_json[self.BLOB_BUCKET_HEALTH_PAGE_KEY]
                metadata_bucket_from_health_page = health_json.get(self.METADATA_BUCKET_HEALTH_PAGE_KEY,
                                                                   # N/A for 4DN
                                                                   None)
                tibanna_output_bucket_from_health_page = health_json.get(self.TIBANNA_OUTPUT_BUCKET_HEALTH_PAGE_KEY,
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
                if tibanna_output_bucket and tibanna_output_bucket != tibanna_output_bucket_from_health_page:
                    raise InferredBucketConflict(kind="tibanna output", specified=tibanna_output_bucket,
                                                 inferred=tibanna_output_bucket_from_health_page)
                else:
                    tibanna_output_bucket = tibanna_output_bucket_from_health_page
                logger.warning('Buckets resolved successfully.')
            else:
                # staging and production share same buckets
                if env:
                    if is_stg_or_prd_env(env):
                        self.url = get_beanstalk_real_url(env)
                        env = prod_bucket_env(env)
                    else:
                        env = full_env_name(env)

                def apply_template(template, env):
                    return template % env if "%s" in template else template
                # we use standardized naming schema, so s3 buckets always have same prefix
                sys_bucket = apply_template(self.SYS_BUCKET_TEMPLATE, env)
                outfile_bucket = apply_template(self.OUTFILE_BUCKET_TEMPLATE, env)
                raw_file_bucket = apply_template(self.RAW_BUCKET_TEMPLATE, env)
                blob_bucket = apply_template(self.BLOB_BUCKET_TEMPLATE, env)
                metadata_bucket = apply_template(self.METADATA_BUCKET_TEMPLATE, env)
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


def find_file(name, zipstream):
    for zipped_filename in zipstream.namelist():
        if zipped_filename.endswith(name):
            return zipped_filename
