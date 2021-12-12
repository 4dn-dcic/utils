import boto3
import functools
import json
import os
import re

from botocore.exceptions import HTTPClientError, ClientError
from typing import Optional
from urllib.parse import urlparse
from . import env_utils_legacy as legacy
from .common import EnvName, OrchestratedApp
from .env_base import EnvManager
from .env_utils_legacy import ALLOW_ENVIRON_BY_DEFAULT
from .exceptions import EnvUtilsLoadError
from .misc_utils import decorator, full_object_name, ignored, remove_prefix, check_true, find_association


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

    :param unimplemented: a boolean saying if orchestrated functionality is unimplemented (so gets an automatic error).
    :param use_legacy: a boolean saying if the legacy definition is suitably general to be used directly
    :param assumes_cgap: a boolean saying whether the orchestrated definition is CGAP-specific
    :param assumes_no_mirror: a boolean saying whether the orchestrated definition is expected to fail if mirroring on
    """

    def _decorate(fn):

        EnvUtils.init()

        legacy_fn = getattr(legacy, fn.__name__)

        if use_legacy:
            return legacy_fn

        @functools.wraps(legacy_fn)
        def wrapped(*args, **kwargs):
            if EnvUtils.IS_LEGACY:
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
                    return legacy_fn(*args, **kwargs)

        return wrapped

    return _decorate


class EnvNames:
    DEV_DATA_SET_TABLE = 'dev_data_set_table'  # dictionary mapping envnames to their preferred data set
    DEV_ENV_DOMAIN_SUFFIX = 'dev_env_domain_suffix'  # e.g., .abc123def456ghi789.us-east-1.rds.amazonaws.com
    FOURSIGHT_URL_PREFIX = 'foursight_url_prefix'
    FULL_ENV_PREFIX = 'full_env_prefix'  # a string like "cgap-" that precedes all env names
    HOTSEAT_ENVS = 'hotseat_envs'  # a list of environments that are for testing with hot data
    INDEXER_ENV_NAME = 'indexer_env_name'  # the environment name used for indexing
    IS_LEGACY = 'is_legacy'
    STAGE_MIRRORING_ENABLED = 'stage_mirroring_enabled'
    ORCHESTRATED_APP = 'orchestrated_app'  # This allows us to tell 'cgap' from 'fourfront', in case there ever is one.
    PRD_BUCKET = 'prd_bucket'
    PRD_ENV_NAME = 'prd_env_name'  # the name of the prod env
    PUBLIC_URL_TABLE = 'public_url_table'  # dictionary mapping envnames & pseudo_envnames to public urls
    STG_ENV_NAME = 'stg_env_name'  # the name of the stage env (or None)
    TEST_ENVS = 'test_envs'  # a list of environments that are for testing


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

    DEV_DATA_SET_TABLE = None  # dictionary mapping envnames to their preferred data set
    DEV_ENV_DOMAIN_SUFFIX = None
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
    PRD_BUCKET = None
    PRD_ENV_NAME = None  # the name of the prod env
    PUBLIC_URL_TABLE = None  # dictionary mapping envnames & pseudo_envnames to public urls
    STG_ENV_NAME = None  # the name of the stage env (or None)
    TEST_ENVS = None  # a list of environments that are for testing

    DEV_SUFFIX_FOR_TESTING = ".abc123def456ghi789.us-east-1.rds.amazonaws.com"

    SAMPLE_TEMPLATE_FOR_CGAP_TESTING = {
        e.DEV_DATA_SET_TABLE: {'acme-hotseat': 'prod', 'acme-test': 'test'},
        e.DEV_ENV_DOMAIN_SUFFIX: DEV_SUFFIX_FOR_TESTING,
        e.FOURSIGHT_URL_PREFIX: 'https://foursight.genetics.example.com/api/view/',
        e.FULL_ENV_PREFIX: 'acme-',
        e.HOTSEAT_ENVS: ['acme-hotseat', 'acme-pubdemo'],
        e.INDEXER_ENV_NAME: 'acme-indexer',
        e.IS_LEGACY: False,
        e.ORCHESTRATED_APP: 'cgap',
        e.PRD_BUCKET: 'production-data',
        e.PRD_ENV_NAME: 'acme-prd',
        e.PUBLIC_URL_TABLE: [
            {
                p.NAME: 'cgap',
                p.URL: 'https://genetics.example.com',
                p.HOST: 'genetics.example.com',
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
        # e.STG_ENV_NAME: None,
        e.TEST_ENVS: ['acme-test', 'acme-mastertest', 'acme-pubtest'],
    }

    SAMPLE_TEMPLATE_FOR_FOURFRONT_TESTING = {
        e.DEV_DATA_SET_TABLE: {'acme-hotseat': 'prod', 'acme-test': 'test'},
        e.DEV_ENV_DOMAIN_SUFFIX: DEV_SUFFIX_FOR_TESTING,
        e.FOURSIGHT_URL_PREFIX: 'https://foursight.genetics.example.com/api/view/',
        e.FULL_ENV_PREFIX: 'acme-',
        e.HOTSEAT_ENVS: ['acme-hotseat'],
        e.INDEXER_ENV_NAME: 'acme-indexer',
        e.IS_LEGACY: False,
        e.ORCHESTRATED_APP: 'fourfront',
        e.PRD_BUCKET: 'production-data',
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
        # e.STG_ENV_NAME: None,
        e.TEST_ENVS: ['acme-test', 'acme-mastertest', 'acme-pubtest'],
    }

    @classmethod
    def init(cls):
        if cls._DECLARED_DATA is None:
            cls.load_declared_data()

    @classmethod
    def declared_data(cls):
        cls.init()
        return cls._DECLARED_DATA

    @classmethod
    def set_declared_data(cls, data):
        cls._DECLARED_DATA = data
        for var, key in EnvNames.__dict__.items():
            if var.isupper():
                if key in data:
                    setattr(EnvUtils, var, data[key])
                else:
                    setattr(EnvUtils, var, None)

    # Vaguely, the thing we're trying to recognize is this (sanitized slightly here),
    # which we first call str() on and then check with a regular expression:
    #
    # ValueError("Invalid header value b'AWS4-HMAC-SHA256 Credential=ABCDEF1234PQRST99999\\n"
    #            "AKIA5NVXX3EKMJOCZ4VE/20211021/us-east-1/s3/aws4_request,"
    #            " SignedHeaders=host;x-amz-content-sha256;x-amz-date;x-amz-security-token,"
    #            " Signature=123abc456def789xyz1a2b3c4d'",)
    _CREDENTIALS_ERROR_PATTERN = re.compile(".*Invalid header value.*(Credentials|SignedHeaders)")

    @classmethod
    def load_declared_data(cls, env_name=None):
        bucket = EnvManager.global_env_bucket_name()
        if bucket:
            env_name = env_name or os.environ.get('ENV_NAME')
            if env_name:
                try:
                    s3 = boto3.client('s3')
                    metadata = s3.get_object(Bucket=bucket, Key=env_name)
                    data = json.load(metadata['Body'])
                    cls.set_declared_data(data)
                    return
                except HTTPClientError as err_obj:
                    if cls._CREDENTIALS_ERROR_PATTERN.match(str(err_obj)):  # Some sort of plausibility test
                        raise EnvUtilsLoadError("Credentials problem, perhaps expired or wrong account.",
                                                bucket_name=bucket, env=env_name, encapsulated_error=err_obj)
                    raise
                except ClientError as err_obj:
                    response_data = getattr(err_obj, 'response', {})
                    response_error = response_data.get('Error', {})
                    if response_error:
                        response_error_code = response_error.get('Code', None)
                        msg = response_error.get('Message') or response_error_code or "Load Error"
                        raise EnvUtilsLoadError(msg, bucket_name=bucket, env=env_name, encapsulated_error=err_obj)
                    raise
                except Exception as err_obj:
                    raise EnvUtilsLoadError(str(err_obj), bucket_name=bucket, env=env_name, encapsulated_error=err_obj)

        cls.set_declared_data({e.IS_LEGACY: True})

    @classmethod
    def lookup(cls, entry, default=_MISSING):
        if default is _MISSING:
            return cls.declared_data()[entry]
        else:
            return cls.declared_data().get(entry, default)


@if_orchestrated
def is_indexer_env(envname: EnvName) -> bool:
    """
    Returns true if the given envname is the indexer env name.
    """
    return envname == EnvUtils.INDEXER_ENV_NAME


@if_orchestrated
def indexer_env_for_env(envname: EnvName) -> Optional[EnvName]:
    """
    Given any environment, returns the associated indexer env.
    (If the environment is the indexer env itself, returns None.)
    """
    if envname == EnvUtils.INDEXER_ENV_NAME:
        return None
    else:
        return EnvUtils.INDEXER_ENV_NAME


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


@if_orchestrated(use_legacy=True)
def blue_green_mirror_env(envname):
    """
    Given a blue envname, returns its green counterpart, or vice versa.
    For other envnames that aren't blue/green participants, this returns None.
    """
    ignored(envname)


@if_orchestrated()
def prod_bucket_env_for_app(appname: Optional[OrchestratedApp] = None):
    _check_appname(appname)
    return prod_bucket_env(EnvUtils.PRD_ENV_NAME)


@if_orchestrated
def prod_bucket_env(envname: EnvName) -> Optional[EnvName]:
    if is_stg_or_prd_env(envname):
        return EnvUtils.PRD_BUCKET
    else:
        return None


@if_orchestrated()
def default_workflow_env(orchestrated_app: OrchestratedApp) -> EnvName:
    _check_appname(orchestrated_app, required=True)
    return EnvUtils.PRD_BUCKET


@if_orchestrated
def get_bucket_env(envname):
    if is_stg_or_prd_env(envname):
        return EnvUtils.PRD_BUCKET
    else:
        return envname


@if_orchestrated
def public_url_mappings(envname):
    ignored(envname)
    return {entry[p.NAME]: entry[p.URL] for entry in EnvUtils.PUBLIC_URL_TABLE}


def _check_appname(appname: Optional[OrchestratedApp], required=False):
    if appname or required:  # VERY IMPORTANT: Only do this check if we are given a specific app name (or it's required)
        if appname != EnvUtils.ORCHESTRATED_APP:
            raise RuntimeError(f"The orchestrated app is {EnvUtils.ORCHESTRATED_APP}, not {appname}.")


@if_orchestrated
def public_url_for_app(appname: Optional[OrchestratedApp] = None) -> Optional[str]:
    _check_appname(appname)
    entry = find_association(EnvUtils.PUBLIC_URL_TABLE, **{p.ENVIRONMENT: EnvUtils.PRD_ENV_NAME})
    if entry:
        return entry.get('url')


@if_orchestrated
def is_cgap_server(server, allow_localhost=False) -> bool:
    check_true(isinstance(server, str), "The 'url' argument must be a string.", error_class=ValueError)
    is_cgap = EnvUtils.ORCHESTRATED_APP == 'cgap'
    if not is_cgap:
        return False
    kind = classify_server_url(server, raise_error=False).get(c.KIND)
    if kind == 'cgap':
        return True
    elif allow_localhost and kind == 'localhost':
        return True
    else:
        return False


@if_orchestrated
def is_fourfront_server(server, allow_localhost=False) -> bool:
    check_true(isinstance(server, str), "The 'url' argument must be a string.", error_class=ValueError)
    is_fourfront = EnvUtils.ORCHESTRATED_APP == 'fourfront'
    if not is_fourfront:
        return False
    kind = classify_server_url(server, raise_error=False).get(c.KIND)
    if kind == 'fourfront':
        return True
    elif allow_localhost and kind == 'localhost':
        return True
    else:
        return False


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


@if_orchestrated
def is_stg_or_prd_env(envname: Optional[EnvName]) -> bool:
    # The legacy version does something much more elaborate that involves heuristics on names.
    # Note, too, that in the legacy version, this would return true both of CGAP or Fourfront names,
    # but really you're only supposed to call it on one or the other's environments. It's rarely the
    # case that the same code would ever be implicating both.  So here we assume every orchestrated
    # ecosystem is one or the other, and hence there is a canonical PRD_ENV_NAME and STG_ENV_NAME.
    # -kmp 24-Jul-2021

    if not isinstance(envname, str):
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


@if_orchestrated(use_legacy=True)
def get_env_from_context(settings, allow_environ=True):
    # The legacy handler will look in settings or in an environemnt variable. Probably that's OK for us.
    ignored(settings, allow_environ)


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
def infer_foursight_url_from_env(request, envname: Optional[EnvName]):
    ignored(request)
    return EnvUtils.FOURSIGHT_URL_PREFIX + short_env_name(envname)


@if_orchestrated
def infer_foursight_from_env(request, envname):
    ignored(request)
    if EnvUtils.STAGE_MIRRORING_ENABLED and EnvUtils.STG_ENV_NAME:
        classification = classify_server_url(request.domain)
        if classification[c.IS_STG_OR_PRD]:
            public_name = classification[c.PUBLIC_NAME]
            if public_name:
                return public_name
    entry = find_association(EnvUtils.PUBLIC_URL_TABLE, environment=envname)
    if entry:
        envname = entry[p.NAME]
    return remove_prefix(EnvUtils.FULL_ENV_PREFIX, envname, required=False)


@if_orchestrated()
def short_env_name(envname: Optional[EnvName]):
    if not envname:
        return None

    return remove_prefix(EnvUtils.FULL_ENV_PREFIX, envname, required=False)


@if_orchestrated
def full_env_name(envname):
    check_true(isinstance(envname, str), "The envname is not a string.", error_class=ValueError)

    entry = find_association(EnvUtils.PUBLIC_URL_TABLE, name=envname)
    if entry:
        return entry[p.ENVIRONMENT]

    if envname.startswith(EnvUtils.FULL_ENV_PREFIX):
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


@if_orchestrated(use_legacy=True)
def make_env_name_cfn_compatible(env_name):
    ignored(env_name)
