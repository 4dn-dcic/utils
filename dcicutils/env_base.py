import boto3
# import botocore.client
import contextlib
# import json
# import logging
import os
# import urllib.request

# from typing import Optional
from .common import LEGACY_GLOBAL_ENV_BUCKET
from .exceptions import (
    # CannotInferEnvFromManyGlobalEnvs,
    # MissingGlobalEnv,
    SynonymousEnvironmentVariablesMismatched, LegacyDispatchDisabled,
)
from .misc_utils import (
    override_environ,
    # ignored,
    remove_suffix,
)


class LegacyController:
    LEGACY_DISPATCH_ENABLED = False


class EnvBase:

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

    @classmethod
    def _legacy_global_env_bucket_for_testing(cls):
        if not LegacyController.LEGACY_DISPATCH_ENABLED:
            raise LegacyDispatchDisabled(operation="_legacy_global_env_bucket_for_testing", mode='setup-envbase')
        # Strictly speaking, this isn't accessing the legacy state, but it's definitely not accessin gproduction state,
        # so it does not want to be used in production. -kmp 18-Jul-2022
        return LEGACY_GLOBAL_ENV_BUCKET

    @classmethod
    def _get_configs(cls, env_bucket, kind):
        env_bucket_name = (
            # prefer a given bucket
            env_bucket
            # or GLOBAL_ENV_BUCKET
            or cls.global_env_bucket_name()
            # but failing that, for legacy system, just use legacy name
            or cls._legacy_global_env_bucket_for_testing())
        s3_resource = boto3.resource('s3')
        env_bucket_model = s3_resource.Bucket(env_bucket_name)
        key_names = [key_obj.key for key_obj in env_bucket_model.objects.all()]
        configs = cls.filter_config_names(key_names, kind=kind)
        return sorted(configs)

    @classmethod
    def filter_config_names(cls, configs, kind):
        result = []
        for config in configs:
            if kind == 'env' and '.' not in config:
                result.append(config)
            elif kind == 'ecosystem' and config.endswith('.ecosystem'):
                result.append(remove_suffix('.ecosystem', config))
        return result

    @classmethod
    def get_all_environments(cls, env_bucket=None):
        return cls._get_configs(env_bucket=env_bucket, kind='env')

    @classmethod
    def get_all_ecosystems(cls, env_bucket=None):
        return cls._get_configs(env_bucket=env_bucket, kind='ecosystem')


class s3Base:

    # Some extra variables used in setup here so that other modules can be consistent with chosen values.

    SYS_BUCKET_SUFFIX = "system"
    OUTFILE_BUCKET_SUFFIX = "wfoutput"
    RAW_BUCKET_SUFFIX = "files"
    BLOB_BUCKET_SUFFIX = "blobs"
    METADATA_BUCKET_SUFFIX = "metadata-bundles"
    TIBANNA_OUTPUT_BUCKET_SUFFIX = 'tibanna-output'
    TIBANNA_CWLS_BUCKET_SUFFIX = 'tibanna-cwls'

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

    EB_PREFIX = "elasticbeanstalk"
    EB_AND_ENV_PREFIX = EB_PREFIX + "-%s-"  # = "elasticbeanstalk-%s-"

    SYS_BUCKET_TEMPLATE = EB_AND_ENV_PREFIX + SYS_BUCKET_SUFFIX            # = "elasticbeanstalk-%s-system"
    OUTFILE_BUCKET_TEMPLATE = EB_AND_ENV_PREFIX + OUTFILE_BUCKET_SUFFIX    # = "elasticbeanstalk-%s-wfoutput"
    RAW_BUCKET_TEMPLATE = EB_AND_ENV_PREFIX + RAW_BUCKET_SUFFIX            # = "elasticbeanstalk-%s-files"
    BLOB_BUCKET_TEMPLATE = EB_AND_ENV_PREFIX + BLOB_BUCKET_SUFFIX          # = "elasticbeanstalk-%s-blobs"
    METADATA_BUCKET_TEMPLATE = EB_AND_ENV_PREFIX + METADATA_BUCKET_SUFFIX  # = "elasticbeanstalk-%s-metadata-bundles"
    TIBANNA_OUTPUT_BUCKET_TEMPLATE = TIBANNA_OUTPUT_BUCKET_SUFFIX          # = "tibanna-output" (no prefix)
    TIBANNA_CWLS_BUCKET_TEMPLATE = TIBANNA_CWLS_BUCKET_SUFFIX              # = "tibanna-cwls" (no prefix)
