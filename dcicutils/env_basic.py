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
    SynonymousEnvironmentVariablesMismatched,
)
from .misc_utils import (
    override_environ,
    # ignored,
    remove_suffix,
)


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
    def _get_configs(cls, env_bucket, kind):
        env_bucket_name = (
            # prefer a given bucket
            env_bucket
            # or GLOBAL_ENV_BUCKET
            or cls.global_env_bucket_name()
            # but failing that, for legacy system, just use legacy name
            or LEGACY_GLOBAL_ENV_BUCKET)
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
