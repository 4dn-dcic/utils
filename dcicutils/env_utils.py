import boto3
import functools
import json
import os

from .misc_utils import decorator, full_object_name, ignored, remove_prefix, check_true
from . import env_utils_legacy as legacy
from urllib.parse import urlparse


class UseLegacy(BaseException):
    """
    Raise this class inside an @if_orchestrated definition in order to dynamically go to using legacy behavior,
    usually after having considered or attempted some different way of doing these things first.  If you want to
    go straight to that, it's better (more efficienct) to use @if_orchestrated(use_legacy=True).

    This class inherits from BaseException, not Exception, because it doesn't want to be trapped by any other
    exception handlers. It wants to dive straight through them and go straight to alternative handling.
    """
    pass


@decorator()
def if_orchestrated(unimplemented=False, use_legacy=False, assumes_cgap=False, assumes_no_mirror=False):
    """
    This is a decorator intended to manage new versions of these functions as they apply to orchestrated CGAP
    without disturbing legacy behavior (now found in env_utils_legacy.py and still supported for the original
    beanstalk-oriented deploys at HMS for cgap-portal AND fourfront.

    The arguments to this decorator are as follows:



    """
    # assumes_cgap and assumes_no_mirror are purely decorative.
    #    assumes_cgap says that the orchestrated handler for this definition presumes a cgap-style orchestration.
    #                 If we ever orchestrate fourfront, definitions marked assumes_cgap need more thought.
    #    assumes_no_mirror says that the orchestrated handler for this definition presumes no fourfront-style
    #                      mirroring. If we ever decide to do such mirroring, we'll need to reconsider
    #                      things marked assumes_no_mirror=True
    ignored(assumes_cgap, assumes_no_mirror)

    def _decorate(fn):

        legacy_fn = getattr(legacy, fn.__name__)

        if use_legacy:
            return legacy_fn

        @functools.wraps(legacy_fn)
        def wrapped(*args, **kwargs):
            if EnvUtils.declared_data().get(e.IS_LEGACY):
                return legacy_fn(*args, **kwargs)
            elif unimplemented:
                raise NotImplementedError(f"Unimplemented: {full_object_name(fn)}")
            else:
                try:
                    return fn(*args, **kwargs)
                except UseLegacy:
                    return legacy_fn(*args, **kwargs)

        return wrapped

    return _decorate


class EnvNames:
    DEV_DATA_SET_TABLE = 'dev_data_set_table'  # dictionary mapping envnames to their preferred data set
    FULL_ENV_PREFIX = 'full_env_prefix'  # a string like "cgap-" that precedes all env names
    HOTSEAT_ENVS = 'hotseat_envs'  # a list of environments that are for testing with hot data
    INDEXER_ENV_NAME = 'indexer_env_name'  # the environment name used for indexing
    IS_LEGACY = 'is_legacy'
    OTHER_CGAP_SERVERS = 'other_cgap_servers'  # server hostnames that don't contain 'cgap' in them but are still CGAP
    OTHER_FOURFRONT_SERVERS = 'other_fourfront_servers'  # server hostnames that are Fourfront even if not obvious
    PRD_ENV_NAME = 'prd_env_name'  # the name of the prod env
    PUBLIC_URL_TABLE = 'public_url_table'  # dictionary mapping envnames & pseudo_envnames to public urls
    # STG_ENV_NAME = 'stg_env_name'  # the name of the stage env (or None)
    TEST_ENVS = 'test_envs'  # a list of environments that are for testing


e = EnvNames


_MISSING = object()


class EnvUtils:

    _DECLARED_DATA = None

    SAMPLE_TEMPLATE_FOR_TESTING = {
        e.DEV_DATA_SET_TABLE: {'cgap': 'prod', 'cgap-test': 'test'},
        e.FULL_ENV_PREFIX: '',
        e.HOTSEAT_ENVS: [],
        e.INDEXER_ENV_NAME: 'cgap-indexer',
        e.IS_LEGACY: False,
        e.OTHER_CGAP_SERVERS: [],
        # We don't have to specify this for now because we're only doing CGAP.
        # If we were doing an orchestrated Fourfront, we'd want this.
        # e.OTHER_FOURFRONT_SERVERS: [],
        e.PRD_ENV_NAME: 'cgap',
        e.PUBLIC_URL_TABLE: {'cgap': 'http://cgap.example.com'},
        # We don't do stage mirroring in the orchestrated world.
        # e.STG_ENV_NAME: None,
        e.TEST_ENVS: [],
    }

    @classmethod
    def declared_data(cls):
        if cls._DECLARED_DATA is None:
            cls.load_declared_data()
        return cls._DECLARED_DATA

    @classmethod
    def set_declared_data(cls, data):
        cls._DECLARED_DATA = data

    @classmethod
    def load_declared_data(cls):
        bucket = os.environ.get('GLOBAL_BUCKET_ENV') or os.environ.get('GLOBAL_ENV_BUCKET')
        if bucket:
            env_name = os.environ.get('ENV_NAME')
            if env_name:
                s3 = boto3.client('s3')
                metadata = s3.get_object(Bucket=bucket, Key=env_name)
                data = json.load(metadata['Body'])
                cls.set_declared_data(data)
                return
        cls.set_declared_data({e.IS_LEGACY: True})

    @classmethod
    def lookup(cls, entry, default=_MISSING):
        if default is _MISSING:
            return cls.declared_data()[entry]
        else:
            return cls.declared_data().get(entry, default)


@if_orchestrated
def is_indexer_env(envname):
    return envname == EnvUtils.lookup(e.INDEXER_ENV_NAME)


@if_orchestrated
def indexer_env_for_env(envname):
    indexer_env = EnvUtils.lookup(e.INDEXER_ENV_NAME)
    if envname == indexer_env:
        return None
    else:
        return indexer_env


@if_orchestrated
def data_set_for_env(envname, default=None):
    if is_stg_or_prd_env(envname):
        return 'prod'
    else:
        info = EnvUtils.lookup(e.DEV_DATA_SET_TABLE) or {}
        return info.get(envname, default)


@if_orchestrated(use_legacy=True)
def blue_green_mirror_env(envname):
    # The legacy definition just swaps the names 'blue' and 'green' in envname, which is fine.
    ignored(envname)


@if_orchestrated
def prod_bucket_env(envname):
    if is_stg_or_prd_env(envname):
        return EnvUtils.lookup(e.PRD_ENV_NAME)
    else:
        return None


@if_orchestrated
def get_bucket_env(envname):
    # This may look the same as the legacy definition, but it has to call our prod_bucket_env, not the legacy one.
    return prod_bucket_env(envname) if is_stg_or_prd_env(envname) else envname


@if_orchestrated
def public_url_mappings(envname):
    ignored(envname)
    mappings = EnvUtils.lookup(e.PUBLIC_URL_TABLE)
    return mappings


@if_orchestrated
def is_cgap_server(server, allow_localhost=False):
    ignored(allow_localhost)
    others = EnvUtils.lookup(e.OTHER_CGAP_SERVERS)
    if server in others:
        return True
    else:
        raise UseLegacy()


@if_orchestrated
def is_fourfront_server(server, allow_localhost=False):
    ignored(allow_localhost)
    others = EnvUtils.lookup(e.OTHER_FOURFRONT_SERVERS)
    if server in others:
        return True
    else:
        raise UseLegacy()


@if_orchestrated(use_legacy=True)
def is_cgap_env(envname):
    # The legacy handler just checks for 'cgap' in the envname, which is fine.
    ignored(envname)


@if_orchestrated(use_legacy=True)
def is_fourfront_env(envname):
    # The legacy handler just checks for 'fourfront' in the envname and 'cgap' not there, which is fine.
    ignored(envname)


@if_orchestrated
def is_stg_or_prd_env(envname):
    # The legacy version does something much more elaborate that involves heuristics on names.
    # We'll just declare the one we want and leave it at that.
    # Note also that we're NOT doing:
    #   return (envname == EnvUtils.lookup(e.STG_ENV_NAME) or
    #           envname == EnvUtils.lookup(e.PRD_ENV_NAME))
    # because there is no mirroring here. Any stage environment is not like legacy cgap stage.
    return envname == EnvUtils.lookup(e.PRD_ENV_NAME)


@if_orchestrated
def is_test_env(envname):
    envs = EnvUtils.lookup(e.TEST_ENVS, default=[])
    return envname in envs if envname else False


@if_orchestrated
def is_hotseat_env(envname):
    envs = EnvUtils.lookup(e.HOTSEAT_ENVS, default=[])
    return envname in envs if envname else False


@if_orchestrated(use_legacy=True)
def get_env_from_context(settings, allow_environ=True):
    # The legacy handler will look in settings or in an environemnt variable. Probably that's OK for us.
    ignored(settings, allow_environ)


@if_orchestrated(assumes_no_mirror=True)
def get_mirror_env_from_context(settings, allow_guess=True):
    ignored(allow_guess, settings)
    # NOTE: This presumes we're not doing mirroring in orchestrated environments.
    #       IF we were, this would need further review.
    return None


@if_orchestrated(assumes_no_mirror=True)
def get_standard_mirror_env(envname):
    ignored(envname)
    # NOTE: This presumes we're not doing mirroring in orchestrated environments.
    #       IF we were, this would need further review.
    return None


@if_orchestrated(assumes_no_mirror=True)
def guess_mirror_env(envname):
    ignored(envname)
    return None


@if_orchestrated
def infer_repo_from_env(envname):
    if not envname:
        return None
    if is_cgap_env(envname):
        return 'cgap-portal'
    elif is_fourfront_env(envname):
        return 'fourfront'
    else:
        return None


@if_orchestrated(assumes_cgap=True)
def infer_foursight_from_env(request, envname):
    ignored(request)
    if is_cgap_env(envname):
        return remove_prefix('cgap-', envname, required=False)
    else:
        raise UseLegacy()


@if_orchestrated
def full_env_name(envname):
    prefix = EnvUtils.lookup(e.FULL_ENV_PREFIX)
    if envname.startswith(prefix):
        return envname
    else:
        return prefix + envname


@if_orchestrated
def full_cgap_env_name(envname):
    check_true(isinstance(envname, str) and is_cgap_env(envname), "The envname is not a CGAP env name.",
               error_class=ValueError)
    return full_env_name(envname)


@if_orchestrated(unimplemented=True, assumes_cgap=True)
def full_fourfront_env_name(envname):
    ignored(envname)


@if_orchestrated
def classify_server_url(url, raise_error=True):

    parsed = urlparse(url)
    hostname = parsed.hostname
    hostname1 = hostname.split('.', 1)[0]  # The part before the first dot (if any)

    environment = get_bucket_env(hostname1)  # First approximation, maybe overridden below

    is_stg_or_prd = is_stg_or_prd_env(hostname1)

    if hostname1 == 'localhost' or hostname == '127.0.0.1':
        environment = 'unknown'
        kind = 'localhost'
    elif 'cgap' in hostname1:
        kind = 'cgap'
    elif is_stg_or_prd or 'fourfront-' in hostname1:
        kind = 'fourfront'
    else:
        if raise_error:
            raise RuntimeError("%s is not a Fourfront or CGAP server." % url)
        else:
            environment = 'unknown'
            kind = 'unknown'

    return {
        'kind': kind,
        'environment': environment,
        'is_stg_or_prd': is_stg_or_prd
    }


@if_orchestrated(use_legacy=True)
def make_env_name_cfn_compatible(env_name):
    ignored(env_name)
