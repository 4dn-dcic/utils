import boto3
import contextlib
import copy
import functools
import json
import os
import re

from botocore.exceptions import HTTPClientError, ClientError
from typing import Optional
from urllib.parse import urlparse
from . import env_utils_legacy as legacy
from .common import (
    EnvName, OrchestratedApp, APP_FOURFRONT, ChaliceStage, CHALICE_STAGE_DEV, CHALICE_STAGE_PROD,
)
from .env_base import EnvBase, LegacyController
from .env_utils_legacy import ALLOW_ENVIRON_BY_DEFAULT
from .exceptions import (
    EnvUtilsLoadError, BeanstalkOperationNotImplemented, MissingFoursightBucketTable, IncompleteFoursightBucketTable,
    LegacyDispatchDisabled,
)
from .misc_utils import (
    decorator, full_object_name, ignored, ignorable, remove_prefix, remove_suffix, check_true, find_association,
    override_environ, get_setting_from_context,
)
from .secrets_utils import assumed_identity_if  # , GLOBAL_APPLICATION_CONFIGURATION


ignorable(BeanstalkOperationNotImplemented)  # Stuff that does or doesn't use this might come and go


class UseLegacy(BaseException):
    """
    Raise this class inside an @if_orchestrated definition in order to dynamically go to using legacy behavior,
    usually after having considered or attempted some different way of doing these things first.  If you want to
    go straight to that, it's better (more efficienct) to use @if_orchestrated(use_legacy=True).

    This class inherits from BaseException, not Exception, because it doesn't want to be trapped by any other
    exception handlers. It wants to dive straight through them and go straight to alternative handling.
    """
    pass


def _make_no_legacy(fn, function_name):
    @functools.wraps(fn)
    def _missing_legacy_function(*args, **kwargs):
        ignored(*args, **kwargs)
        raise NotImplementedError(f"There is only an orchestrated version of {function_name}, not a legacy version."
                                  f" args={args} kwargs={kwargs}")
    return _missing_legacy_function


@decorator()
def if_orchestrated(*, unimplemented=False, use_legacy=False, assumes_cgap=False, assumes_no_mirror=False):
    """
    This is a decorator intended to manage new versions of these functions as they apply to orchestrated CGAP
    without disturbing legacy behavior (now found in env_utils_legacy.py and still supported for the original
    beanstalk-oriented deploys at HMS for cgap-portal AND fourfront.

    The arguments to this decorator are as follows:

    :param unimplemented: a boolean saying if orchestrated functionality is unimplemented (so gets an automatic error).
    :param use_legacy: a boolean saying if the legacy definition is suitably general to be used directly instead
    :param assumes_cgap: a boolean saying whether the orchestrated definition is CGAP-specific
    :param assumes_no_mirror: a boolean saying whether the orchestrated definition is expected to fail if mirroring on
    """

    def _decorate(fn):

        # EnvUtils.init(raise_load_errors=False)

        orchestrated_function_name = fn.__name__

        try:
            legacy_fn = getattr(legacy, orchestrated_function_name)
        except AttributeError:
            # If is no legacy function (e.g., for new functionality), conjure a function with nicer error message
            legacy_fn = _make_no_legacy(fn, orchestrated_function_name)

        if use_legacy:
            if not LegacyController.LEGACY_DISPATCH_ENABLED:
                raise LegacyDispatchDisabled(operation=orchestrated_function_name, mode='decorate')
            return legacy_fn

        @functools.wraps(legacy_fn)
        def wrapped(*args, **kwargs):

            EnvUtils.init()  # In case this is the first time

            if EnvUtils.IS_LEGACY:
                if not LegacyController.LEGACY_DISPATCH_ENABLED:
                    raise LegacyDispatchDisabled(operation=orchestrated_function_name,
                                                 mode='dispatch', call_args=args, call_kwargs=kwargs)
                return legacy_fn(*args, **kwargs)
            elif unimplemented:
                raise NotImplementedError(f"Unimplemented: {full_object_name(fn)}")
            elif assumes_no_mirror and EnvUtils.STAGE_MIRRORING_ENABLED:
                raise NotImplementedError(f"In {full_object_name(fn)}:"
                                          f" Mirroring is not supported in an orchestrated environment.")
            elif assumes_cgap and EnvUtils.ORCHESTRATED_APP != 'cgap':
                raise NotImplementedError(f"In {full_object_name(fn)}:"
                                          f" Non-cgap applications are not supported.")
            else:
                try:
                    return fn(*args, **kwargs)
                except UseLegacy:
                    if not LegacyController.LEGACY_DISPATCH_ENABLED:
                        raise LegacyDispatchDisabled(operation=orchestrated_function_name,
                                                     mode='raised', call_args=args, call_kwargs=kwargs)

                    return legacy_fn(*args, **kwargs)

        return wrapped

    return _decorate


ENV_DEFAULT = 'default'


class EnvNames:
    DEFAULT_WORKFLOW_ENV = 'default_workflow_env'
    DEV_DATA_SET_TABLE = 'dev_data_set_table'  # dictionary mapping envnames to their preferred data set
    DEV_ENV_DOMAIN_SUFFIX = 'dev_env_domain_suffix'  # e.g., .abc123def456ghi789.us-east-1.rds.amazonaws.com
    ECOSYSTEM = 'ecosystem'  # name of an ecosystem file, such as "main.ecosystem"
    FOURSIGHT_BUCKET_PREFIX = 'foursight_bucket_prefix'
    FOURSIGHT_URL_PREFIX = 'foursight_url_prefix'
    FOURSIGHT_BUCKET_TABLE = 'foursight_bucket_table'
    FULL_ENV_PREFIX = 'full_env_prefix'  # a string like "cgap-" that precedes all env names
    HOTSEAT_ENVS = 'hotseat_envs'  # a list of environments that are for testing with hot data
    INDEXER_ENV_NAME = 'indexer_env_name'  # the environment name used for indexing
    IS_LEGACY = 'is_legacy'
    STAGE_MIRRORING_ENABLED = 'stage_mirroring_enabled'
    ORCHESTRATED_APP = 'orchestrated_app'  # This allows us to tell 'cgap' from 'fourfront', in case there ever is one.
    PRD_ENV_NAME = 'prd_env_name'  # the name of the prod env
    PRODUCTION_ECR_REPOSITORY = "production_ecr_repository"  # the name of an ecr repository shared between stg and prd
    PUBLIC_URL_TABLE = 'public_url_table'  # dictionary mapping envnames & pseudo_envnames to public urls
    STG_ENV_NAME = 'stg_env_name'  # the name of the stage env (or None)
    TEST_ENVS = 'test_envs'  # a list of environments that are for testing
    WEBPROD_PSEUDO_ENV = 'webprod_pseudo_env'


class PublicUrlParts:
    ENVIRONMENT = 'environment'
    HOST = 'host'
    NAME = 'name'
    URL = 'url'


class ClassificationParts:

    BUCKET_ENV = 'bucket_env'
    ENVIRONMENT = 'environment'  # Obsolete. Use BUCKET_ENV or SERVER_ENV going forward
    IS_STG_OR_PRD = 'is_stg_or_prd'
    KIND = 'kind'
    PUBLIC_NAME = 'public_name'
    SERVER_ENV = 'server_env'


e = EnvNames
p = PublicUrlParts
c = ClassificationParts

_MISSING = object()


class EnvUtils:
    """
    This class offers internal bookeeping support for the env_utils functionality.

    ************************* NOTE WELL ******************************
    This class is not intended for use outside this file. Its name does not begin with a leading underscore
    to mark it as internal, but that's more a practical accomodation to the number of places it's used across
    several files within this repository. But this implementation needs to be able to change in the future
    without such change being regarded as incompatible. All functionality here should be accessed through the
    functions exported by env_utils (which will often use this class internally) but never directly.
    ******************************************************************
    """

    _DECLARED_DATA = None

    DEFAULT_WORKFLOW_ENV = None
    DEV_DATA_SET_TABLE = None  # dictionary mapping envnames to their preferred data set
    DEV_ENV_DOMAIN_SUFFIX = None
    FOURSIGHT_BUCKET_TABLE = None
    FOURSIGHT_BUCKET_PREFIX = None
    FOURSIGHT_URL_PREFIX = None
    FULL_ENV_PREFIX = None  # a string like "cgap-" that precedes all env names (does NOT have to include 'cgap')
    HOTSEAT_ENVS = None
    INDEXER_ENV_NAME = None  # the environment name used for indexing
    IS_LEGACY = None
    # Don't enable this casually. It's intended only if we make some decision to engage mirroring.
    # Although it's fine to call an environment your staging environment without enabling this,
    # what the system means about something being the stg environment is that it's the stage side of a mirror.
    # -kmp 24-Jul-2021
    STAGE_MIRRORING_ENABLED = None  # if True, orchestration-enabled function may offer mirroring behavior
    ORCHESTRATED_APP = None  # This allows us to tell 'cgap' from 'fourfront', in case there ever is one.
    PRD_ENV_NAME = None  # the name of the prod env
    PRODUCTION_ECR_REPOSITORY = None  # the name of an ecr repository shared between stg and prd
    PUBLIC_URL_TABLE = None  # dictionary mapping envnames & pseudo_envnames to public urls
    STG_ENV_NAME = None  # the name of the stage env (or None)
    TEST_ENVS = None  # a list of environments that are for testing
    WEBPROD_PSEUDO_ENV = None

    DEV_SUFFIX_FOR_TESTING = ".abc123def456ghi789.us-east-1.rds.amazonaws.com"

    SAMPLE_TEMPLATE_FOR_CGAP_TESTING = {
        e.DEV_DATA_SET_TABLE: {'acme-hotseat': 'prod', 'acme-test': 'test'},
        e.DEV_ENV_DOMAIN_SUFFIX: DEV_SUFFIX_FOR_TESTING,
        e.FOURSIGHT_BUCKET_TABLE: {
            "acme-prd": {CHALICE_STAGE_DEV: "acme-foursight-dev-prd",
                         CHALICE_STAGE_PROD: "acme-foursight-prod-prd"},
            "acme-stg": {CHALICE_STAGE_DEV: "acme-foursight-dev-stg",
                         CHALICE_STAGE_PROD: "acme-foursight-prod-stg"},
            ENV_DEFAULT: {CHALICE_STAGE_DEV: "acme-foursight-dev-other",
                          CHALICE_STAGE_PROD: "acme-foursight-prod-other"},
        },
        e.FOURSIGHT_URL_PREFIX: 'https://foursight.genetics.example.com/api/view/',
        e.FULL_ENV_PREFIX: 'acme-',
        e.HOTSEAT_ENVS: ['acme-hotseat', 'acme-pubdemo'],
        e.INDEXER_ENV_NAME: 'acme-indexer',
        e.IS_LEGACY: False,
        e.ORCHESTRATED_APP: 'cgap',
        e.PRD_ENV_NAME: 'acme-prd',
        e.PUBLIC_URL_TABLE: [
            {
                p.NAME: 'cgap',
                p.URL: 'https://cgap.genetics.example.com',
                p.HOST: 'cgap.genetics.example.com',
                p.ENVIRONMENT: 'acme-prd',
            },
            {
                p.NAME: 'stg',
                p.URL: 'https://staging.genetics.example.com',
                p.HOST: 'staging.genetics.example.com',
                p.ENVIRONMENT: 'acme-stg',
            },
            {
                p.NAME: 'testing',
                p.URL: 'https://testing.genetics.example.com',
                p.HOST: 'testing.genetics.example.com',
                p.ENVIRONMENT: 'acme-pubtest',
            },
            {
                p.NAME: 'demo',
                p.URL: 'https://demo.genetics.example.com',
                p.HOST: 'demo.genetics.example.com',
                p.ENVIRONMENT: 'acme-pubdemo',
            },
        ],
        # We don't do stage mirroring in the orchestrated world, and probably should not confuse users by inviting
        # these to be set. For extra security, even if you did set them they wouldn't work without enabling mirroring.
        # -kmp 24-Jul-2021
        # e.STAGE_MIRRORING_ENABLED: False,
        # e.STG_ENV_NAME: None,
        e.TEST_ENVS: ['acme-test', 'acme-mastertest', 'acme-pubtest'],
        e.WEBPROD_PSEUDO_ENV: 'production-data',
        e.DEFAULT_WORKFLOW_ENV: 'acme-dev',
    }

    SAMPLE_TEMPLATE_FOR_FOURFRONT_TESTING = {
        e.DEV_DATA_SET_TABLE: {'acme-hotseat': 'prod', 'acme-test': 'test'},
        e.DEV_ENV_DOMAIN_SUFFIX: DEV_SUFFIX_FOR_TESTING,
        e.FOURSIGHT_BUCKET_TABLE: {
            "acme-prd": {CHALICE_STAGE_DEV: "acme-foursight-dev-prd",
                         CHALICE_STAGE_PROD: "acme-foursight-prod-prd"},
            "acme-stg": {CHALICE_STAGE_DEV: "acme-foursight-dev-stg",
                         CHALICE_STAGE_PROD: "acme-foursight-prod-stg"},
            ENV_DEFAULT: {CHALICE_STAGE_DEV: "acme-foursight-dev-other",
                          CHALICE_STAGE_PROD: "acme-foursight-prod-other"},
        },
        e.FOURSIGHT_URL_PREFIX: 'https://foursight.genetics.example.com/api/view/',
        e.FULL_ENV_PREFIX: 'acme-',
        e.HOTSEAT_ENVS: ['acme-hotseat'],
        e.INDEXER_ENV_NAME: 'acme-indexer',
        e.IS_LEGACY: False,
        e.ORCHESTRATED_APP: 'fourfront',
        e.STAGE_MIRRORING_ENABLED: True,
        e.STG_ENV_NAME: 'acme-stg',
        e.PRD_ENV_NAME: 'acme-prd',
        e.PUBLIC_URL_TABLE: [
            {
                p.NAME: 'data',
                p.URL: 'https://genetics.example.com',
                p.HOST: 'genetics.example.com',
                p.ENVIRONMENT: 'acme-prd',
            },
            {
                p.NAME: 'staging',
                p.URL: 'https://stg.genetics.example.com',
                p.HOST: 'stg.genetics.example.com',
                p.ENVIRONMENT: 'acme-stg',
            },
            {
                p.NAME: 'test',
                p.URL: 'https://testing.genetics.example.com',
                p.HOST: 'testing.genetics.example.com',
                p.ENVIRONMENT: 'acme-pubtest',
            },
            {
                p.NAME: 'demo',
                p.URL: 'https://demo.genetics.example.com',
                p.HOST: 'demo.genetics.example.com',
                p.ENVIRONMENT: 'acme-pubdemo',
            },
            {
                p.NAME: 'hot',
                p.URL: 'https://hot.genetics.example.com',
                p.HOST: 'hot.genetics.example.com',
                p.ENVIRONMENT: 'acme-hotseat',
            },
        ],
        # We don't do stage mirroring in the orchestrated world, and probably should not confuse users by inviting
        # these to be set. For extra security, even if you did set them they wouldn't work without enabling mirroring.
        # -kmp 24-Jul-2021
        # e.STAGE_MIRRORING_ENABLED: False,
        # e.STG_ENV_NAME: None,
        e.TEST_ENVS: ['acme-test', 'acme-mastertest', 'acme-pubtest'],
        e.WEBPROD_PSEUDO_ENV: 'production-data',
        e.PRODUCTION_ECR_REPOSITORY: 'acme-production',
        e.DEFAULT_WORKFLOW_ENV: 'acme-dev',
    }

    @classmethod
    def init(cls, env_name=None, ecosystem=None, force=False, raise_load_errors=True, assuming_identity=True):
        if force or cls._DECLARED_DATA is None:
            cls.load_declared_data(env_name=env_name, ecosystem=ecosystem, raise_load_errors=raise_load_errors,
                                   assuming_identity=assuming_identity)

    @classmethod
    def declared_data(cls):
        cls.init(raise_load_errors=False)
        return cls._DECLARED_DATA

    @classmethod
    def set_declared_data(cls, data):

        if data.get(e.IS_LEGACY) and not LegacyController.LEGACY_DISPATCH_ENABLED:
            raise LegacyDispatchDisabled(operation='set_declared_data', mode='load-env')

        cls._DECLARED_DATA = data
        for var, key in EnvNames.__dict__.items():
            if var.isupper():
                if key in data:
                    setattr(EnvUtils, var, data[key])
                else:
                    setattr(EnvUtils, var, None)

    @classmethod
    def snapshot_envutils_state_for_testing(cls):
        result = {}
        for attr in EnvNames.__dict__.keys():
            if attr.isupper():
                result[attr] = copy.deepcopy(getattr(EnvUtils, attr))
        return result

    @classmethod
    def restore_envutils_state_from_snapshot_for_testing(cls, snapshot):
        for attr, val in snapshot.items():
            setattr(EnvUtils, attr, val)

    @classmethod
    @contextlib.contextmanager
    def temporary_state(cls):
        snapshot = cls.snapshot_envutils_state_for_testing()
        try:
            yield
        finally:
            cls.restore_envutils_state_from_snapshot_for_testing(snapshot)

    @classmethod
    @contextlib.contextmanager
    def local_env_utils_for_testing(cls, global_env_bucket=None, env_name=None, raise_load_errors=True):
        attrs = {}
        if global_env_bucket:
            attrs['GLOBAL_ENV_BUCKET'] = global_env_bucket
        if env_name:
            attrs['ENV_NAME'] = env_name
        with EnvUtils.temporary_state():
            with override_environ(**attrs):
                EnvUtils.init(force=True, raise_load_errors=raise_load_errors, assuming_identity=False)
                yield

    @classmethod
    @contextlib.contextmanager
    def locally_declared_data_for_testing(cls, data=None, **kwargs):
        if data is None:
            data = {}
        with cls.temporary_state():
            if data is not None:
                cls.set_declared_data(data)  # First set the given data, if any
            # Now override individual specified attributes
            for attr, val in kwargs.items():
                setattr(cls, attr, val)
            yield

    @classmethod
    @contextlib.contextmanager
    def fresh_testing_state_from(cls, *, bucket=None, data=None, global_bucket=None):
        with EnvBase.global_env_bucket_named(global_bucket or bucket):
            with EnvUtils.temporary_state():
                if bucket:
                    assert data is None, "You must supply bucket or data, but not both."
                    EnvUtils.init(force=True, assuming_identity=False)
                elif data:
                    EnvUtils.set_declared_data(data)
                else:
                    raise AssertionError("You must supply either bucket or data.")
                yield

    FF_DEPLOYED_BUCKET = 'foursight-prod-envs'
    FF_BUCKET = 'foursight-test-envs'
    CGAP_BUCKET = 'foursight-cgap-envs'

    @classmethod
    @contextlib.contextmanager
    def fresh_ff_deployed_state_for_testing(cls):
        with cls.fresh_testing_state_from(bucket=cls.FF_DEPLOYED_BUCKET):
            yield

    @classmethod
    @contextlib.contextmanager
    def fresh_cgap_deployed_state_for_testing(cls):
        with cls.fresh_testing_state_from(bucket=cls.CGAP_BUCKET):
            yield

    # Vaguely, the thing we're trying to recognize is this (sanitized slightly here),
    # which we first call str() on and then check with a regular expression:
    #
    # ValueError("Invalid header value b'AWS4-HMAC-SHA256 Credential=ABCDEF1234PQRST99999\\n"
    #            "AKIA5NVXX3EKMJOCZ4VE/20211021/us-east-1/s3/aws4_request,"
    #            " SignedHeaders=host;x-amz-content-sha256;x-amz-date;x-amz-security-token,"
    #            " Signature=123abc456def789xyz1a2b3c4d'",)
    _CREDENTIALS_ERROR_PATTERN = re.compile(".*Invalid header value.*(Credentials|SignedHeaders)")

    @classmethod
    def _get_config_object_from_s3(cls, env_bucket: str, config_key: str) -> dict:
        """
         Returns the contents of an environmental configuration file.

        :param env_bucket: the name of a bucket (the global_env_bucket)
        :param config_key: the name of a key (the name of a configuration - an ff_env or ecosystem)
        """
        try:
            s3 = boto3.client('s3')
            metadata = s3.get_object(Bucket=env_bucket, Key=config_key)
            data = json.load(metadata['Body'])
            return data
        except HTTPClientError as err_obj:
            if cls._CREDENTIALS_ERROR_PATTERN.match(str(err_obj)):  # Some sort of plausibility test
                raise EnvUtilsLoadError("Credentials problem, perhaps expired or wrong account.",
                                        env_bucket=env_bucket, config_key=config_key, encapsulated_error=err_obj)
            raise
        except ClientError as err_obj:
            response_data = getattr(err_obj, 'response', {})
            response_error = response_data.get('Error', {})
            if response_error:
                response_error_code = response_error.get('Code', None)
                msg = response_error.get('Message') or response_error_code or "Load Error"
                raise EnvUtilsLoadError(msg, env_bucket=env_bucket, config_key=config_key, encapsulated_error=err_obj)
            raise

    @classmethod
    def _get_config_ecosystem_from_s3(cls, env_bucket, config_key):
        seen = set()
        while config_key not in seen:
            seen.add(config_key)
            config_data = cls._get_config_object_from_s3(env_bucket=env_bucket, config_key=config_key)
            ecosystem = config_data.get(e.ECOSYSTEM)
            if not ecosystem:
                break
            # Indirect to ecosystem
            # Format should be:               "ecosystem": "main"
            # But for now we also tolerate:   "ecosystem": "main.ecosystem"
            config_key = ecosystem
            if config_key and not config_key.endswith(".ecosystem"):
                config_key = f"{ecosystem}.ecosystem"
        ecosystem = config_data.get(e.ECOSYSTEM)  # noQA - PyCharm worries wrongly this won't have been set
        if ecosystem and ecosystem.endswith(".ecosystem"):
            config_data[e.ECOSYSTEM] = remove_suffix(".ecosystem", ecosystem)
        return config_data                        # noQA - PyCharm worries wrongly this won't have been set

    @classmethod
    def load_declared_data(cls, env_name=None, ecosystem=None, raise_load_errors=True, assuming_identity=True):
        with assumed_identity_if(assuming_identity, only_if_missing='GLOBAL_ENV_BUCKET'):
            cls._load_declared_data(env_name=env_name, ecosystem=ecosystem, raise_load_errors=raise_load_errors)

    @classmethod
    def _load_declared_data(cls, *, env_name, ecosystem, raise_load_errors):
        """
        Tries to load environmental data from any of various keys in the global env bucket.
        1. If an ecosystem was specified, load from <ecosystem>.ecosystem.
        2. If an env_name was specified, load from that name.
        3. If the environment variable ENV_NAME is set, load from that name.
        4. Using the environment variable ECOSYSTEM (default 'main'), load from <ecosystem>.ecosystem
        """
        config_key = f"{ecosystem}.ecosystem" if ecosystem else (env_name or os.environ.get('ENV_NAME'))
        if not config_key:
            config_key = f"{os.environ.get('ECOSYSTEM', 'main')}.ecosystem"
        env_bucket = EnvBase.global_env_bucket_name()
        if env_bucket and config_key:
            try:
                config_data = cls._get_config_ecosystem_from_s3(env_bucket=env_bucket, config_key=config_key)
                cls.set_declared_data(config_data)
                return
            except Exception as err_obj:
                if raise_load_errors:
                    raise EnvUtilsLoadError(str(err_obj),
                                            env_bucket=env_bucket,
                                            config_key=config_key,
                                            encapsulated_error=err_obj)

        cls.set_declared_data({e.IS_LEGACY: True})

    @classmethod
    def lookup(cls, entry, default=_MISSING):
        if default is _MISSING:
            return cls.declared_data()[entry]
        else:
            return cls.declared_data().get(entry, default)


@if_orchestrated
def is_indexer_env(envname: EnvName):
    """
    Returns true if the given envname is the indexer env name.
    """
    ignored(envname)
    return False
    #
    # Formerly:
    #
    #     return envname == EnvUtils.INDEXER_ENV_NAME
    #
    # and we also tried:
    #
    #     raise BeanstalkOperationNotImplemented(operation="indexer_env")


@if_orchestrated
def indexer_env_for_env(envname: EnvName):
    """
    Given any environment, returns the associated indexer env.
    (If the environment is the indexer env itself, returns None.)
    """
    ignored(envname)
    return None

    # Formerly:
    #
    #     if envname == EnvUtils.INDEXER_ENV_NAME:
    #         return None
    #     else:
    #         return EnvUtils.INDEXER_ENV_NAME
    #
    # and we also tried:
    #
    #     raise BeanstalkOperationNotImplemented(operation="indexer_env_for_env")


@if_orchestrated
def data_set_for_env(envname: EnvName, default=None):
    if is_stg_or_prd_env(envname):
        return 'prod'
    else:
        info = EnvUtils.DEV_DATA_SET_TABLE or {}
        return info.get(envname, default)


@if_orchestrated()
def permit_load_data(envname: Optional[EnvName], allow_prod: bool, orchestrated_app: OrchestratedApp):
    ignored(envname, orchestrated_app)
    # This does something way more complicated in legacy systems, but we're experimenting with a simpler rule
    # as our first approximation in an orchestrated environment. -kmp 7-Oct-2021
    return bool(allow_prod)  # in case a non-bool allow_prod value is given, canonicalize the result


@if_orchestrated(use_legacy=False)  # was True until recently
def blue_green_mirror_env(envname):
    """
    Given a blue envname, returns its green counterpart, or vice versa.
    For other envnames that aren't blue/green participants, this returns None.
    """
    # This was copioed from the legacy definition, but we're phasing out legacy now.
    # Odd as the definition is, we're still using this function.
    # See: https://github.com/search?q=blue_green_mirror_env&type=code
    # -kmp 8-Jul-2022
    if 'blue' in envname:
        if 'green' in envname:
            raise ValueError('A blue/green mirror env must have only one of blue or green in its name.')
        return envname.replace('blue', 'green')
    elif 'green' in envname:
        return envname.replace('green', 'blue')
    else:
        return None


@if_orchestrated()
def prod_bucket_env_for_app(appname: Optional[OrchestratedApp] = None):
    _check_appname(appname)
    return prod_bucket_env(EnvUtils.PRD_ENV_NAME)


def prod_bucket_env(envname: EnvName) -> None:
    ignored(envname)
    raise BeanstalkOperationNotImplemented(operation='prod_bucket_env')

# @if_orchestrated
# def prod_bucket_env(envname: EnvName) -> Optional[EnvName]:
#     if is_stg_or_prd_env(envname):
#         if EnvUtils.WEBPROD_PSEUDO_ENV:
#             return EnvUtils.WEBPROD_PSEUDO_ENV
#         elif EnvUtils.STAGE_MIRRORING_ENABLED and envname == EnvUtils.STG_ENV_NAME:
#             return EnvUtils.PRD_ENV_NAME
#         return envname
#     else:  # For a non-prod env, we just return None
#         return None


@if_orchestrated()
def default_workflow_env(orchestrated_app: OrchestratedApp) -> EnvName:
    """
    Returns the default workflow environment to be used in testing (and with ill-constructed .ini files)
    for WorkFlowRun situations in the portal when there is no env.name in the registry.
    """
    _check_appname(orchestrated_app, required=True)
    return (EnvUtils.DEFAULT_WORKFLOW_ENV
            or (EnvUtils.HOTSEAT_ENVS[0]
                if EnvUtils.HOTSEAT_ENVS
                else None)
            or EnvUtils.PRD_ENV_NAME)


@if_orchestrated()
def ecr_repository_for_env(envname):
    if is_stg_or_prd_env(envname):
        # Prefer a declared name if there is one.
        return EnvUtils.PRODUCTION_ECR_REPOSITORY or envname
    else:
        return envname


@if_orchestrated
def get_bucket_env(envname):
    if is_stg_or_prd_env(envname):
        return EnvUtils.WEBPROD_PSEUDO_ENV
    else:
        return envname


@if_orchestrated
def public_url_mappings(envname):
    ignored(envname)
    return {entry[p.NAME]: entry[p.URL] for entry in EnvUtils.PUBLIC_URL_TABLE}


def _check_appname(appname: Optional[OrchestratedApp], required=False):
    if appname or required:  # VERY IMPORTANT: Only do this check if we are given a specific app name (or it's required)
        # ALSO: Only check if the orchestrated app is declared. Otherwise, trust.
        if EnvUtils.ORCHESTRATED_APP and appname != EnvUtils.ORCHESTRATED_APP:
            raise RuntimeError(f"The orchestrated app is {EnvUtils.ORCHESTRATED_APP}, not {appname}.")


@if_orchestrated
def public_url_for_app(appname: Optional[OrchestratedApp] = None) -> Optional[str]:
    _check_appname(appname)
    entry = find_association(EnvUtils.PUBLIC_URL_TABLE, **{p.ENVIRONMENT: EnvUtils.PRD_ENV_NAME})
    if entry:
        return entry.get('url')


def _find_public_url_entry(envname):
    entry = (find_association(EnvUtils.PUBLIC_URL_TABLE, **{p.NAME: envname}) or
             find_association(EnvUtils.PUBLIC_URL_TABLE, **{p.ENVIRONMENT: full_env_name(envname)}) or
             find_association(EnvUtils.PUBLIC_URL_TABLE, **{p.ENVIRONMENT: short_env_name(envname)}))
    if entry:
        return entry


@if_orchestrated
def get_env_real_url(envname):

    entry = _find_public_url_entry(envname)
    if entry:
        return entry.get('url')

    if not EnvUtils.DEV_ENV_DOMAIN_SUFFIX:
        raise RuntimeError(f"DEV_ENV_DOMAIN_SUFFIX is not defined."
                           f" It is needed for get_env_real_url({envname!r})"
                           f" because env {envname} has no entry in {EnvNames.PUBLIC_URL_TABLE}.")

    if EnvUtils.ORCHESTRATED_APP == APP_FOURFRONT:  # Only fourfront shortens
        # Fourfront is a low-security application, so only 'data' is 'https' and the rest are 'http'.
        # TODO: This should be table-driven, too, but we're not planning to distribute Fourfront,
        #       so it's not high priority. -kmp 13-May-2022
        protocol = 'https' if is_stg_or_prd_env(envname) else 'http'
        return f"{protocol}://{short_env_name(envname)}{EnvUtils.DEV_ENV_DOMAIN_SUFFIX}"
    else:
        # For CGAP, everything has to be 'https'. Part of our security model.
        return f"https://{envname}{EnvUtils.DEV_ENV_DOMAIN_SUFFIX}"


@if_orchestrated
def is_cgap_server(server, allow_localhost=False) -> bool:
    ignored(server, allow_localhost)
    return EnvUtils.ORCHESTRATED_APP == 'cgap'
    #
    # check_true(isinstance(server, str), "The 'url' argument must be a string.", error_class=ValueError)
    # is_cgap = EnvUtils.ORCHESTRATED_APP == 'cgap'
    # if not is_cgap:
    #     return False
    # kind = classify_server_url(server, raise_error=False).get(c.KIND)
    # if kind == 'cgap':
    #     return True
    # elif allow_localhost and kind == 'localhost':
    #     return True
    # else:
    #     return False


@if_orchestrated
def is_fourfront_server(server, allow_localhost=False) -> bool:
    ignored(server, allow_localhost)
    return EnvUtils.ORCHESTRATED_APP == 'fourfront'
    #
    # check_true(isinstance(server, str), "The 'url' argument must be a string.", error_class=ValueError)
    # is_fourfront = EnvUtils.ORCHESTRATED_APP == 'fourfront'
    # if not is_fourfront:
    #     return False
    # kind = classify_server_url(server, raise_error=False).get(c.KIND)
    # if kind == 'fourfront':
    #     return True
    # elif allow_localhost and kind == 'localhost':
    #     return True
    # else:
    #     return False


@if_orchestrated
def is_cgap_env(envname: Optional[EnvName]) -> bool:
    if not isinstance(envname, str):
        return False
    elif EnvUtils.ORCHESTRATED_APP != 'cgap':
        return False
    elif envname.startswith(EnvUtils.FULL_ENV_PREFIX):
        return True
    elif find_association(EnvUtils.PUBLIC_URL_TABLE, **{p.NAME: envname}):
        return True
    else:
        return False


@if_orchestrated
def is_fourfront_env(envname: Optional[EnvName]) -> bool:
    if not isinstance(envname, str):
        return False
    elif EnvUtils.ORCHESTRATED_APP != 'fourfront':
        return False
    elif envname.startswith(EnvUtils.FULL_ENV_PREFIX):
        return True
    elif find_association(EnvUtils.PUBLIC_URL_TABLE, **{p.NAME: envname}):
        return True
    else:
        return False


@if_orchestrated
def compute_prd_env_for_project(project: OrchestratedApp):
    _check_appname(appname=project)
    return EnvUtils.PRD_ENV_NAME


def _is_raw_stg_or_prd_env(envname: EnvName) -> bool:
    return (envname == EnvUtils.PRD_ENV_NAME or
            (EnvUtils.STAGE_MIRRORING_ENABLED and EnvUtils.STG_ENV_NAME and envname == EnvUtils.STG_ENV_NAME))


@if_orchestrated
def is_orchestrated() -> bool:
    return True


@if_orchestrated
def maybe_get_declared_prd_env_name(project: OrchestratedApp) -> Optional[EnvName]:
    _check_appname(appname=project)
    return EnvUtils.PRD_ENV_NAME


@if_orchestrated
def has_declared_stg_env(project: OrchestratedApp) -> bool:
    _check_appname(appname=project)
    return True if EnvUtils.STG_ENV_NAME else False


# We could inherit the beanstalk definition, but that would mean we can't recycle the envnames in a container account,
# and there's really no reason not to allow that. -kmp 4-May-2022
# @if_orchestrated(use_legacy=True)

@if_orchestrated
def is_beanstalk_env(envname):
    """
    Returns True if envname is one of the traditional/legacy beanstalk names, and False otherwise.

    NOTE: The list of names is heled in _ALL_BEANSTALK_NAMES, but you MUST NOT reference that variable directly.
          Always use this function.
    """
    ignored(envname)
    return False


@if_orchestrated
def is_stg_or_prd_env(envname: Optional[EnvName]) -> bool:
    """
    Returns True if the given envname is the name of something that might be either live data or something
    that is ready to be swapped in as live.  So things like 'staging' and either of 'blue/green' will return
    True whether or not they are actually the currently live instance. (This function doesn't change its
    state as blue or green is deployed, in other words.)
    """
    if not envname:
        return False
    elif _is_raw_stg_or_prd_env(envname):
        return True
    else:
        alias_entry = find_association(EnvUtils.PUBLIC_URL_TABLE, name=envname)
        if alias_entry:
            return _is_raw_stg_or_prd_env(alias_entry[p.ENVIRONMENT])
        else:
            return False


@if_orchestrated
def is_test_env(envname: Optional[EnvName]) -> bool:
    envs = EnvUtils.TEST_ENVS or []
    if not isinstance(envname, str):
        return False
    elif envname in envs:
        return True
    else:
        alias_entry = find_association(EnvUtils.PUBLIC_URL_TABLE, name=envname)
        if alias_entry:
            return alias_entry[p.ENVIRONMENT] in envs
        else:
            return False


@if_orchestrated
def is_hotseat_env(envname: Optional[EnvName]) -> bool:
    envs = EnvUtils.HOTSEAT_ENVS or []
    if not isinstance(envname, str):
        return False
    elif envname in envs:
        return True
    else:
        alias_entry = find_association(EnvUtils.PUBLIC_URL_TABLE, name=envname)
        if alias_entry:
            return alias_entry[p.ENVIRONMENT] in envs
        else:
            return False


@if_orchestrated(use_legacy=False)  # was True until recently
def get_env_from_context(settings, allow_environ=True):
    """Look for an env in settings or in an environemnt variable."""
    # This definition was capied from the lgegacy definition.
    return get_setting_from_context(settings, ini_var='env.name', env_var=None if allow_environ else False)


@if_orchestrated
def get_mirror_env_from_context(settings, allow_environ=ALLOW_ENVIRON_BY_DEFAULT, allow_guess=True):
    # This is the same text as the legacy environment, but it needs to call get_standard_mirror_env
    # from this file. -kmp 26-Jul-2021
    if not EnvUtils.STAGE_MIRRORING_ENABLED:
        return None
    elif allow_environ:
        environ_mirror_env_name = os.environ.get('MIRROR_ENV_NAME')
        if environ_mirror_env_name:
            return environ_mirror_env_name
    declared = settings.get('mirror.env.name', '')
    if declared:
        return declared
    elif allow_guess:
        who_i_am = get_env_from_context(settings, allow_environ=allow_environ)
        return get_standard_mirror_env(who_i_am)
    else:
        return None


def get_foursight_bucket_prefix():
    declared_foursight_bucket_prefix = EnvUtils.FOURSIGHT_BUCKET_PREFIX
    if declared_foursight_bucket_prefix:
        return declared_foursight_bucket_prefix
    bucket_env = EnvBase.global_env_bucket_name()
    if bucket_env and bucket_env.endswith("-envs"):
        return remove_suffix('-envs', bucket_env)
    else:
        raise RuntimeError("No foursight_bucket_prefix is declared and one cannot be inferred.")


@if_orchestrated
def get_foursight_bucket(envname: EnvName, stage: ChaliceStage) -> str:

    bucket = None
    bucket_table = EnvUtils.FOURSIGHT_BUCKET_TABLE

    bucket_table_seen = False
    if isinstance(bucket_table, dict):
        bucket_table_seen = True
        env_entry = bucket_table.get(envname)
        if not env_entry:
            env_entry = bucket_table.get(ENV_DEFAULT)
        if isinstance(env_entry, dict):
            bucket = env_entry.get(stage)
        if bucket:
            return bucket

    if EnvUtils.FOURSIGHT_BUCKET_PREFIX:
        return f"{EnvUtils.FOURSIGHT_BUCKET_PREFIX}-{stage}-{full_env_name(envname)}"

    if bucket_table_seen:
        raise IncompleteFoursightBucketTable(f"No foursight bucket is defined for envname={envname} stage={stage}"
                                             f" in {EnvNames.FOURSIGHT_BUCKET_TABLE}={bucket_table}.")
    else:
        raise MissingFoursightBucketTable(f"No {EnvNames.FOURSIGHT_BUCKET_TABLE} is declared.")


@if_orchestrated
def get_standard_mirror_env(envname):
    if not EnvUtils.STAGE_MIRRORING_ENABLED:
        return None
    elif envname == EnvUtils.PRD_ENV_NAME:
        return EnvUtils.STG_ENV_NAME
    elif envname == EnvUtils.STG_ENV_NAME:
        return EnvUtils.PRD_ENV_NAME
    else:
        entry = find_association(EnvUtils.PUBLIC_URL_TABLE, name=envname)
        if entry:
            real_envname = entry[p.ENVIRONMENT]
            if real_envname == EnvUtils.PRD_ENV_NAME:
                found = EnvUtils.STG_ENV_NAME
            elif real_envname == EnvUtils.STG_ENV_NAME:
                found = EnvUtils.PRD_ENV_NAME
            else:
                return False
            rev_entry = find_association(EnvUtils.PUBLIC_URL_TABLE, environment=found)
            if rev_entry:
                return rev_entry[p.NAME]
            else:
                return found
    return None


# guess_mirror_env was reatained deprecated for a while for compatibility, but
# as of dcicutils 3.0.0, it's gone. Please use get_standard_mirror_env.


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


@if_orchestrated()
def infer_foursight_url_from_env(*, request=None, envname: Optional[EnvName] = None):
    """
    Infers the Foursight URL for the given envname and request context.

    :param request: This argument is
    :param envname: name of the environment we are on
    :return: Foursight env at the end of the url ie: for fourfront-green, could be either 'data' or 'staging'
    """
    ignored(request)
    if envname is None:
        # Although short_env_name allows None, it will return None in that case and a more confusing error will result.
        # (We allow None only so we can gracefully phase out the 'request' argument.) -kmp 15-May-2022
        raise ValueError("A non-null envname is required by infer_foursight_url_from_env.")
    return EnvUtils.FOURSIGHT_URL_PREFIX + infer_foursight_from_env(request=request, envname=envname)


@if_orchestrated
def infer_foursight_from_env(*, request=None, envname: Optional[EnvName] = None, short: bool = True):
    """
    Infers the Foursight environment token to view based on the given envname and request context

    :param request: the current request (or an object that has a 'domain' attribute)
    :param envname: name of the environment we are on
    :param short: whether to shorten the result using short_env_name.
    :return: Foursight env at the end of the url ie: for fourfront-green, could be either 'data' or 'staging'
    """

    # We're phasing out this argument for this operation.  We should perhaps have a separate infer_foursight_from_url
    # that can be used outside of the core operations by something that knows it has a public-facing URL,
    # but this does not work in our orchestrated configuration to make necessary portal decisions because
    # we sometimes receive URLs using hosts that contain explicit references to private IP addresses from
    # which nothing useful can be inferred.  -kmp 20-Jul-2022
    ignored(request)

    if envname is None:
        # We allow None only so we can gracefully phase out the 'request' argument. -kmp 15-May-2022
        raise ValueError("A non-null envname is required by infer_foursight_from_env.")

    # This isn't helpful any more. In Fargate we sometimes just get a random IP address.from
    # The envname information should get us what we need.
    # -kmp 14-Jul-2022
    #
    # # If a request is passed and stage-mirroring is enabled, we can tell from the URL if we're staging
    # # then for anything that is a stg or prd, return its 'public name' token.
    # # TODO: Find a simpler way to write this block of code? It's not very abstract. -kmp 23-May-2022
    # if request and EnvUtils.STAGE_MIRRORING_ENABLED and EnvUtils.STG_ENV_NAME:
    #     classification = classify_server_url(request if isinstance(request, str) else request.domain)
    #     if classification[c.IS_STG_OR_PRD]:
    #         public_name = classification[c.PUBLIC_NAME]
    #         if public_name:
    #             return public_name

    entry = (find_association(EnvUtils.PUBLIC_URL_TABLE, name=envname) or
             find_association(EnvUtils.PUBLIC_URL_TABLE, environment=full_env_name(envname)) or
             find_association(EnvUtils.PUBLIC_URL_TABLE, environment=short_env_name(envname)))
    if entry:
        envname = entry[p.NAME]
    else:
        envname = short_env_name(envname) if short else envname
    return envname


@if_orchestrated()
def short_env_name(envname: Optional[EnvName]):
    if not envname:
        return None

    entry = find_association(EnvUtils.PUBLIC_URL_TABLE, name=envname)
    if entry:
        envname = entry[p.ENVIRONMENT]

    if not EnvUtils.FULL_ENV_PREFIX:  # "" or None
        return envname
    return remove_prefix(EnvUtils.FULL_ENV_PREFIX, envname, required=False)


@if_orchestrated
def full_env_name(envname):
    check_true(isinstance(envname, str), "The envname is not a string.", error_class=ValueError)

    entry = find_association(EnvUtils.PUBLIC_URL_TABLE, name=envname)
    if entry:
        return entry[p.ENVIRONMENT]
    elif (not EnvUtils.FULL_ENV_PREFIX
          or envname.startswith(EnvUtils.FULL_ENV_PREFIX)):
        return envname
    else:
        return EnvUtils.FULL_ENV_PREFIX + envname


@if_orchestrated
def full_cgap_env_name(envname):
    check_true(isinstance(envname, str) and EnvUtils.ORCHESTRATED_APP == 'cgap', "The envname is not a CGAP env name.",
               error_class=ValueError)
    return full_env_name(envname)


@if_orchestrated
def full_fourfront_env_name(envname):
    check_true(isinstance(envname, str) and EnvUtils.ORCHESTRATED_APP == 'fourfront',
               "The envname is not a Fourfront env name.",
               error_class=ValueError)
    return full_env_name(envname)


@if_orchestrated
def classify_server_url(url, raise_error=True):

    public_name = None
    check_true(isinstance(url, str), "The 'url' argument must be a string.", error_class=ValueError)

    if not url.startswith("http"):
        url = "http://" + url  # without a prefix, the url parser does badly
    parsed = urlparse(url)
    hostname = parsed.hostname
    hostname1 = hostname.split('.', 1)[0]  # The part before the first dot (if any)

    if hostname1 == 'localhost' or hostname == '127.0.0.1':
        environment = 'unknown'
        kind = 'localhost'
    elif hostname1.startswith(EnvUtils.FULL_ENV_PREFIX) and hostname.endswith(EnvUtils.DEV_ENV_DOMAIN_SUFFIX):
        environment = hostname1
        kind = EnvUtils.ORCHESTRATED_APP
        entry = find_association(EnvUtils.PUBLIC_URL_TABLE, **{p.ENVIRONMENT: environment})
        if entry:
            public_name = entry[p.NAME]
    else:
        entry = find_association(EnvUtils.PUBLIC_URL_TABLE, **{p.HOST: hostname})
        if entry:
            environment = entry[p.ENVIRONMENT]
            kind = EnvUtils.ORCHESTRATED_APP
            public_name = entry[p.NAME]
        elif raise_error:
            raise RuntimeError(f"{url} is not a {EnvUtils.ORCHESTRATED_APP} server.")
        else:
            environment = 'unknown'
            kind = 'unknown'

    bucket_env = get_bucket_env(environment)  # note that 'unknown' may go through this, returning still 'unknown'
    return {
        c.KIND: kind,
        c.ENVIRONMENT: bucket_env,  # Obsolete. Really a bucket_env, so use that instead.
        c.BUCKET_ENV: bucket_env,
        c.SERVER_ENV: environment,
        c.IS_STG_OR_PRD: is_stg_or_prd_env(environment),  # _is_raw_stg_or_prd_env(environment),
        c.PUBLIC_NAME: public_name,
    }


# I don't think this function is ever called in any repo. -kmp 15-May-2022
#
# @if_orchestrated(use_legacy=True)
# def make_env_name_cfn_compatible(env_name):
#     ignored(env_name)
