import boto3
import json
import logging
import mimetypes
import os

from io import BytesIO
from zipfile import ZipFile
from .base import get_beanstalk_real_url
from .env_base import EnvManager
from .env_utils import is_stg_or_prd_env, prod_bucket_env, full_env_name
from .exceptions import InferredBucketConflict
from .misc_utils import PRINT, exported


# For legacy reasons, other modules or repos might expect these names in this file.
# This isn't a full enumeration of all names they expect to find, though.
exported(get_beanstalk_real_url, EnvManager)


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
    PYTHON_VERSION = 'python_version'
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


def find_file(name, zipstream):
    for zipped_filename in zipstream.namelist():
        if zipped_filename.endswith(name):
            return zipped_filename
