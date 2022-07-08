import os
import structlog

from typing import Optional
from urllib.parse import urlparse
from .common import EnvName, OrchestratedApp, APP_CGAP, APP_FOURFRONT, ORCHESTRATED_APPS
from .exceptions import InvalidParameterError
from .misc_utils import get_setting_from_context, check_true, remove_prefix


logger = structlog.getLogger(__name__)


# Mechanism for returning app-dependent values.
def _orchestrated_app_case(orchestrated_app: OrchestratedApp, if_cgap, if_fourfront):
    if orchestrated_app == APP_CGAP:
        return if_cgap
    elif orchestrated_app == APP_FOURFRONT:
        return if_fourfront
    else:
        raise InvalidParameterError(parameter='orchestrated_app', value=orchestrated_app, options=ORCHESTRATED_APPS)


FF_ENV_DEV = 'fourfront-dev'  # Maybe not used
FF_ENV_HOTSEAT = 'fourfront-hotseat'
FF_ENV_MASTERTEST = 'fourfront-mastertest'
FF_ENV_PRODUCTION_BLUE = 'fourfront-blue'
FF_ENV_PRODUCTION_GREEN = 'fourfront-green'
FF_ENV_STAGING = 'fourfront-staging'
FF_ENV_WEBDEV = 'fourfront-webdev'
FF_ENV_WEBPROD = 'fourfront-webprod'
FF_ENV_WEBPROD2 = 'fourfront-webprod2'
FF_ENV_WOLF = 'fourfront-wolf'
FF_ENV_INDEXER = 'fourfront-indexer'  # to be used by ELB Indexer

FF_ENV_PRODUCTION_BLUE_NEW = 'fourfront-production-blue'
FF_ENV_PRODUCTION_GREEN_NEW = 'fourfront-production-green'

CGAP_ENV_DEV = 'fourfront-cgapdev'
CGAP_ENV_HOTSEAT = 'fourfront-cgaphotseat'  # Maybe not used
CGAP_ENV_MASTERTEST = 'fourfront-cgaptest'
CGAP_ENV_PRODUCTION_BLUE = 'fourfront-cgap-blue'  # reserved for transition use
CGAP_ENV_PRODUCTION_GREEN = 'fourfront-cgap-green'  # reserved for transition use
CGAP_ENV_STAGING = 'fourfront-cgapstaging'
CGAP_ENV_WEBDEV = 'fourfront-cgapwebdev'  # Maybe not used
CGAP_ENV_WEBPROD = 'fourfront-cgap'
# CGAP_ENV_WEBPROD2 is meaningless here. See CGAP_ENV_STAGING.
CGAP_ENV_WOLF = 'fourfront-cgapwolf'  # Maybe not used
CGAP_ENV_INDEXER = 'cgap-indexer'  # to be used by ELB Indexer

CGAP_ENV_DEV_NEW = 'cgap-dev'
CGAP_ENV_HOTSEAT_NEW = 'cgap-hotseat'
CGAP_ENV_MASTERTEST_NEW = 'cgap-test'
CGAP_ENV_PRODUCTION_BLUE_NEW = 'cgap-blue'
CGAP_ENV_PRODUCTION_GREEN_NEW = 'cgap-green'
CGAP_ENV_STAGING_NEW = 'cgap-staging'
CGAP_ENV_WEBDEV_NEW = 'cgap-webdev'  # Maybe not used
CGAP_ENV_WEBPROD_NEW = 'cgap-green'
# CGAP_ENV_WEBPROD2_NEW is meaningless here. See CGAP_ENV_STAGING_NEW.
CGAP_ENV_WOLF_NEW = 'cgap-wolf'  # Maybe not used

_ALL_BEANSTALK_NAMES = [
    # You can peek at contents of this variable to see how is_beanstalk_env will work,
    # but DO NOT reference this directly from code outside of this module. -kmp 18-Apr-2022

    # Fourfront beansstalks
    'fourfront-webprod',      # FF_ENV_WEBPROD (superseded by fourfront-green)
    'fourfront-webprod2',     # FF_ENV_WEBPROD2 (superseded by fourfront-blue)
    'fourfront-green',        # FF_ENV_PRODUCTION_GREEN
    'fourfront-blue',         # FF_ENV_PRODUCTION_BLUE
    'fourfront-mastertest',   # FF_ENV_MASTERTEST
    'fourfront-webdev',       # FF_ENV_WEBDEV
    'fourfront-hotseat',      # FF_ENV_HOTSEAT
    # CGAP beanstalks
    'fourfront-cgap',         # CGAP_ENV_WEBPROD (decommissioned, replaced by AWS orchestration)
    'fourfront-cgapdev',      # CGAP_ENV_DEV (decommissioned, replaced by AWS orchestration)
    'fourfront-cgaptest',     # CGAP_ENV_MASTERTEST (decommissioned, replaced by AWS orchestration)
    'fourfront-cgapwolf',     # CGAP_ENV_WOLF (decommissioned, replaced by AWS orchestration
]

# The bucket names were allocated originally and needn't change.

FF_PROD_BUCKET_ENV = FF_ENV_WEBPROD
CGAP_PROD_BUCKET_ENV = CGAP_ENV_WEBPROD

# Done this way to get maximally compatible behavior.
FOURFRONT_STG_OR_PRD_TOKENS = ['webprod', 'blue', 'green']
FOURFRONT_STG_OR_PRD_NAMES = ['staging', 'stagging', 'data']

# We should know which BS Envs are indexing envs
INDEXER_ENVS = [FF_ENV_INDEXER, CGAP_ENV_INDEXER]

# Done this way because it's safer going forward.
CGAP_STG_OR_PRD_TOKENS = []
CGAP_STG_OR_PRD_NAMES = [CGAP_ENV_WEBPROD, CGAP_ENV_PRODUCTION_GREEN, CGAP_ENV_PRODUCTION_BLUE,
                         CGAP_ENV_PRODUCTION_GREEN_NEW, CGAP_ENV_PRODUCTION_BLUE_NEW,
                         'cgap']


FF_PUBLIC_URL_STG = 'http://staging.4dnucleome.org'
FF_PUBLIC_URL_PRD = 'https://data.4dnucleome.org'
FF_PUBLIC_DOMAIN_STG = 'staging.4dnucleome.org'
FF_PUBLIC_DOMAIN_PRD = 'data.4dnucleome.org'
FF_PRODUCTION_IDENTIFIER = 'data'
FF_STAGING_IDENTIFIER = 'staging'

FF_PUBLIC_URLS = {
    'staging': FF_PUBLIC_URL_STG,
    'data': FF_PUBLIC_URL_PRD,
}

# These names are recently changed but are only used internally to this repo. I did a github-wide search.
_CGAP_MGB_PUBLIC_URL_STG = 'https://staging.cgap-mgb.hms.harvard.edu'  # A stopgap for testing that may have to change
_CGAP_MGB_PUBLIC_URL_PRD = 'https://cgap-mgb.hms.harvard.edu'
_CGAP_MGB_PUBLIC_DOMAIN_PRD = 'cgap.hms.harvard.edu'
_CGAP_MGB_PRODUCTION_IDENTIFIER = 'cgap'

# This table exists to keep legacy use of names that help through blue/green deployments on Fourfront, and that
# left us room (that might one day be needed) for the option of such deployments on CGAP, too. -kmp 26-Apr-2022
CGAP_PUBLIC_URLS = {
    'cgap': _CGAP_MGB_PUBLIC_URL_PRD,
    'fourfront-cgap': _CGAP_MGB_PUBLIC_URL_PRD,
    'data': _CGAP_MGB_PUBLIC_URL_PRD,
    'staging': _CGAP_MGB_PUBLIC_URL_STG,
}

BEANSTALK_PROD_BUCKET_ENVS = {
    'staging': FF_PROD_BUCKET_ENV,
    'data': FF_PROD_BUCKET_ENV,
    FF_ENV_WEBPROD: FF_PROD_BUCKET_ENV,
    FF_ENV_WEBPROD2: FF_PROD_BUCKET_ENV,
    FF_ENV_PRODUCTION_BLUE: FF_PROD_BUCKET_ENV,
    FF_ENV_PRODUCTION_GREEN: FF_PROD_BUCKET_ENV,
    FF_ENV_PRODUCTION_BLUE_NEW: FF_PROD_BUCKET_ENV,
    FF_ENV_PRODUCTION_GREEN_NEW: FF_PROD_BUCKET_ENV,
    'cgap': CGAP_PROD_BUCKET_ENV,
    CGAP_ENV_PRODUCTION_BLUE: CGAP_PROD_BUCKET_ENV,
    CGAP_ENV_PRODUCTION_GREEN: CGAP_PROD_BUCKET_ENV,
    CGAP_ENV_WEBPROD: CGAP_PROD_BUCKET_ENV,
    CGAP_ENV_PRODUCTION_BLUE_NEW: CGAP_PROD_BUCKET_ENV,
    CGAP_ENV_PRODUCTION_GREEN_NEW: CGAP_PROD_BUCKET_ENV,
}

BEANSTALK_PROD_MIRRORS = {

    FF_ENV_PRODUCTION_BLUE: FF_ENV_PRODUCTION_GREEN,
    FF_ENV_PRODUCTION_GREEN: FF_ENV_PRODUCTION_BLUE,
    FF_ENV_PRODUCTION_BLUE_NEW: FF_ENV_PRODUCTION_GREEN_NEW,
    FF_ENV_PRODUCTION_GREEN_NEW: FF_ENV_PRODUCTION_BLUE_NEW,
    FF_ENV_WEBPROD: FF_ENV_WEBPROD2,
    FF_ENV_WEBPROD2: FF_ENV_WEBPROD,

    'staging': 'data',
    'data': 'staging',

    CGAP_ENV_PRODUCTION_BLUE: CGAP_ENV_PRODUCTION_GREEN,
    CGAP_ENV_PRODUCTION_GREEN: CGAP_ENV_PRODUCTION_BLUE,
    CGAP_ENV_WEBPROD: None,

    CGAP_ENV_PRODUCTION_BLUE_NEW: CGAP_ENV_PRODUCTION_GREEN_NEW,
    CGAP_ENV_PRODUCTION_GREEN_NEW: CGAP_ENV_PRODUCTION_BLUE_NEW,

    'cgap': None,

}

BEANSTALK_TEST_ENVS = [

    FF_ENV_HOTSEAT,
    FF_ENV_MASTERTEST,
    FF_ENV_WEBDEV,
    FF_ENV_WOLF,

    CGAP_ENV_HOTSEAT,
    CGAP_ENV_MASTERTEST,
    CGAP_ENV_WEBDEV,
    CGAP_ENV_WOLF,

    CGAP_ENV_HOTSEAT_NEW,
    CGAP_ENV_MASTERTEST_NEW,
    CGAP_ENV_WEBDEV_NEW,
    CGAP_ENV_WOLF_NEW,

]

BEANSTALK_DEV_DATA_SETS = {

    'fourfront-hotseat': 'prod',
    'fourfront-mastertest': 'test',
    'fourfront-webdev': 'prod',

    'fourfront-cgapdev': 'test',
    'fourfront-cgaptest': 'prod',
    'fourfront-cgapwolf': 'prod',

    'cgap-dev': 'test',
    'cgap-test': 'prod',
    'cgap-wolf': 'prod',

}


def is_indexer_env(envname: EnvName):
    """ Checks whether envname is an indexer environment.

    :param envname:  envname to check
    :return: True if envname is an indexer application, False otherwise
    """
    return envname in [FF_ENV_INDEXER, CGAP_ENV_INDEXER]


def indexer_env_for_env(envname: EnvName):
    """ Returns the corresponding indexer-env name for the given env.

    :param envname: envname we want to determine the indexer for
    :returns: either FF_ENV_INDEXER or CGAP_ENV_INDEXER or None
    """
    if is_fourfront_env(envname) and envname != FF_ENV_INDEXER:
        return FF_ENV_INDEXER
    elif is_cgap_env(envname) and envname != CGAP_ENV_INDEXER:
        return CGAP_ENV_INDEXER
    else:
        return None


def data_set_for_env(envname: Optional[EnvName], default=None):
    """
    This relates to which data set to load.
    For production environments, really the null set is loaded because the data is already there and trusted.
    This must always work for all production environments and there is deliberately no provision to override that.
    In all other environments, the question is whether to load ADDITIONAL data, and that's a kind of custom choice,
    so we consult a table for now, pending a better theory of organization.
    """
    if is_stg_or_prd_env(envname):
        return 'prod'
    else:
        return BEANSTALK_DEV_DATA_SETS.get(envname, default)


def permit_load_data(envname: Optional[EnvName], allow_prod: bool, orchestrated_app: OrchestratedApp):
    """ Returns True on whether or not load_data should proceed (presumably in a load-data command line operation).

    :param envname: env we are on
    :param allow_prod: prod argument supplied with '--prod', defaults to False
    :param orchestrated_app: a string token to indicate which app we're using (either 'cgap' or 'fourfront')
    :return: True if load_data should continue, False otherwise
    """

    if orchestrated_app == 'cgap':

        # run on cgaptest
        if envname == CGAP_ENV_MASTERTEST:
            logger.info('load_data: proceeding since we are on %s' % envname)
            return True

        if envname and not allow_prod:  # old logic, allow run on servers if --prod is specified
            logger.info('load_data: skipping, since on %s' % envname)
            return False

        # Allow run on local, which will not have env set, or if --prod was given.
        logger.info('load_data: proceeding since we are either on local or specified the prod option')
        return True

    elif orchestrated_app == 'fourfront':

        # do not run on a production environment unless we set --prod flag
        if is_stg_or_prd_env(envname) and not allow_prod:
            logger.info('load_data: skipping, since we are on a production environment and --prod not used')
            return False

        # do not run on hotseat since it is a prod snapshot
        if 'hotseat' in envname:
            logger.info('load_data: skipping, since we are on hotseat')
            return False

        return True

    else:
        raise InvalidParameterError(parameter='orchestrated_app', value=orchestrated_app, options=['cgap', 'fourfront'])


def blue_green_mirror_env(envname: EnvName):
    """
    Given a blue envname, returns its green counterpart, or vice versa.
    For other envnames that aren't blue/green participants, this returns None.
    """
    if 'blue' in envname:
        if 'green' in envname:
            raise ValueError('A blue/green mirror env must have only one of blue or green in its name.')
        return envname.replace('blue', 'green')
    elif 'green' in envname:
        return envname.replace('green', 'blue')
    else:
        return None


def public_url_for_app(appname: Optional[OrchestratedApp] = None):
    """
    Returns the public production URL for the given application.
    If no application is given, the legacy beanstalk default is 'fourfront',
    but the orchestrated default will be the currently orchestrated app.
    That's weird because it means that prod_bucket_env_for_app() will return 'http://data.4dnucleome.org' even for cgap
    when using beanstalks, but that's what we want for compatibility purposes.
    This will all be better in containers.
    Passing an explicit argument can still obtain the cgap prod bucket.

    :param appname: the application name token ('cgap' or 'fourfront')
    """

    if appname is None:
        appname = 'fourfront'
    return _orchestrated_app_case(orchestrated_app=appname,
                                  if_cgap=_CGAP_MGB_PUBLIC_URL_PRD,
                                  if_fourfront=FF_PUBLIC_URL_PRD)


def prod_bucket_env_for_app(appname: Optional[OrchestratedApp] = None):
    """
    Returns the prod bucket app for a given application name.
    If no application is given, the legacy beanstalk default is 'fourfront',
    but the orchestrated default will be the currently orchestrated app.
    That's weird because it means that prod_bucket_env_for_app() will return 'fourfront-webprod' even for cgap
    when using beanstalks, but that's what we want for compatibility purposes.
    This will all be better in containers.
    Passing an explicit argument can still obtain the cgap prod bucket.

    :param appname: the name of the app (either 'cgap' or 'fourfront')
    """
    if appname is None:
        appname = 'fourfront'
    return _orchestrated_app_case(orchestrated_app=appname,
                                  if_cgap=CGAP_PROD_BUCKET_ENV,
                                  if_fourfront=FF_PROD_BUCKET_ENV)


def prod_bucket_env(envname: Optional[EnvName] = None):
    """
    Given a production-class envname returns the envname of the associated production bucket.
    For other envnames that aren't production envs, this returns None.

    The envname is something that is either a staging or production env, in particular something
    that is_stg_or_prd_env returns True for.

    This is intended for use when configuring a beanstalk. This functionality is agnostic
    about whether we're asking on behalf of CGAP or Fourfront, and whether we're using an old or new
    naming scheme. Just give the current envname as an argument, and it will know (by declaration,
    see the BEANSTALK_PROD_ENV_BUCKET_TOKENS table) what the appropriate production bucket name token is for
    that ecosystem.
    """
    return BEANSTALK_PROD_BUCKET_ENVS.get(envname)


def default_workflow_env(orchestrated_app: OrchestratedApp) -> EnvName:
    """
    Given orchestrated app ('cgap' or 'fourfront'), this returns the default env name (in case None is supplied)
    to use for actual and simulated tibanna workflow runs.
    """
    return _orchestrated_app_case(orchestrated_app=orchestrated_app,
                                  if_cgap=CGAP_ENV_WOLF,
                                  if_fourfront=FF_ENV_WEBDEV)


FF_PRODUCTION_ECR_REPOSITORY = 'fourfront-production'


def ecr_repository_for_env(envname: EnvName):
    # This wasn't originally needed in the legacy environment, which didn't have ECR repos.
    # We handle this case so that if an env was improperly bootstrapped, there is a plausible
    # return value for the operation.
    if 'cgap' in envname:
        result = envname
    elif is_stg_or_prd_env(envname):
        result = FF_PRODUCTION_ECR_REPOSITORY
    else:
        result = envname
    logger.warning(f"ecr_repository_for_env({envname!r}) called in legacy mode. Returning {result!r}.")
    return result


def get_bucket_env(envname: EnvName):
    return prod_bucket_env(envname) if is_stg_or_prd_env(envname) else envname


def public_url_mappings(envname: EnvName):
    """
    Returns a table of the public URLs we use for the ecosystem in which the envname resides.
    For example, if envname is a CGAP URL, this returns a set table of CGAP public URLs,
    and otherwise it returns a set of Fourfront URLs.

    The envname may be 'cgap', 'data', 'staging', or an environment name.
    """
    return CGAP_PUBLIC_URLS if is_cgap_env(envname) else FF_PUBLIC_URLS


def get_env_real_url(envname):
    if 'cgap' in envname:
        # For CGAP, everything has to be 'https'. Part of our security model.
        return f"https://{short_env_name(envname)}.hms.harvard.edu"
    else:  # presumably Fourfront
        # For Fourfront, we're a little more flexible on the security (for now).
        protocol = 'https' if is_stg_or_prd_env(envname) else 'http'
        return f"{protocol}://{short_env_name(envname)}.4dnucleome.org"


def is_cgap_server(server, allow_localhost=False):
    """
    Returns True if the given string looks like a CGAP server name. Otherwise returns False.

    If allow_localhost (default False) is True, then 'localhost' will be treated as a CGAP host.
    """
    check_true(isinstance(server, str), "Server name must be a string.", error_class=ValueError)
    return 'cgap' in server or (allow_localhost and 'localhost' in server)


def is_fourfront_server(server, allow_localhost=False):
    """
    Returns True if the given string looks like a Fourfront server name. Otherwise returns False.

    If allow_localhost (default False) is True, then 'localhost' will be treated as a Fourfront host.
    """
    check_true(isinstance(server, str), "Server name must be a string.", error_class=ValueError)
    return (("fourfront" in server or "4dnucleome" in server) and not is_cgap_server(server)
            or (allow_localhost and 'localhost' in server))


def is_cgap_env(envname: EnvName):
    """
    Returns True of the given string looks like a CGAP elasticbeanstalk environment name.
    Otherwise returns False.
    """
    return 'cgap' in envname if envname else False


def is_fourfront_env(envname: EnvName):
    """
    Returns True of the given string looks like a Fourfront elasticbeanstalk environment name.
    Otherwise returns False.
    """
    return ('fourfront' in envname and 'cgap' not in envname) if envname else False


def is_orchestrated():
    return False


def maybe_get_declared_prd_env_name(project: OrchestratedApp) -> Optional[EnvName]:
    return _orchestrated_app_case(orchestrated_app=project,
                                  if_fourfront=None,
                                  if_cgap='fourfront-cgap')


def has_declared_stg_env(project: OrchestratedApp) -> bool:
    return _orchestrated_app_case(orchestrated_app=project,
                                  if_fourfront=True,
                                  if_cgap=False)


def is_beanstalk_env(envname):
    """
    Returns True if envname is one of the traditional/legacy beanstalk names, and False otherwise.

    NOTE: The list of names is heled in _ALL_BEANSTALK_NAMES, but you MUST NOT reference that variable directly.
          Always use this function.
    """
    return envname in _ALL_BEANSTALK_NAMES


def is_stg_or_prd_env(envname: Optional[EnvName]):
    """
    Returns True if the given envname is the name of something that might be either live data or something
    that is ready to be swapped in as live.  So things like 'staging' and either of 'blue/green' will return
    True whether or not they are actually the currently live instance. (This function doesn't change its
    state as blue or green is deployed, in other words.)
    """
    if not envname:
        return False
    stg_or_prd_tokens = CGAP_STG_OR_PRD_TOKENS if is_cgap_env(envname) else FOURFRONT_STG_OR_PRD_TOKENS
    stg_or_prd_names = CGAP_STG_OR_PRD_NAMES if is_cgap_env(envname) else FOURFRONT_STG_OR_PRD_NAMES
    if envname in stg_or_prd_names:
        return True
    elif any(token in envname for token in stg_or_prd_tokens):
        return True
    return False


def is_test_env(envname: EnvName):
    return envname in BEANSTALK_TEST_ENVS if envname else False


def is_hotseat_env(envname: EnvName):
    return 'hot' in envname if envname else False


# TODO: This variable and all the 'allow_environ=' arguments could go away in the next major version release.
#       --Kent & Will 15-Apr-2020
ALLOW_ENVIRON_BY_DEFAULT = True


def get_env_from_context(settings, allow_environ=ALLOW_ENVIRON_BY_DEFAULT):
    """Look for an env in settings or in an environemnt variable."""
    return get_setting_from_context(settings, ini_var='env.name', env_var=None if allow_environ else False)


def get_mirror_env_from_context(settings, allow_environ=ALLOW_ENVIRON_BY_DEFAULT, allow_guess=True):
    """
    Figures out who the mirror beanstalk Env is if applicable
    This is important in our production environment because in our
    blue-green deployment we maintain two elasticsearch intances that
    must be up to date with each other.
    """
    if allow_environ:
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


def get_standard_mirror_env(envname: EnvName):
    """
    This function knows about the standard mirroring rules and infers a mirror env only from that.
    (In tha sense, it is not guessing and probably needs to be renamed.)
    If there is no mirror, it returns None.

    An envname is usually a beanstalk environment name,
    but this will also swap the special mirror names like 'staging' <=> 'data'.
    For cgap, it may return None until/unless we start using a mirror for that, but the important thing
    there is that again it will return a mirror if there is one, and otherwise None.

    This is not the same as blue_green_mirror_env(envname), which is purely syntactic.

    This is also not the same as get_mirror_env_from_context(...), which infers the mirror from contextual
    information such as config files and environment variables.
    """
    return BEANSTALK_PROD_MIRRORS.get(envname, None)


# guess_mirror_env was reatained for a while for compatibility. Please prefer get_standard_mirror_env.
#
# def guess_mirror_env(envname):
#     """
#     Deprecated. This function returns what get_standard_mirror_env(envname) returns.
#     (The name guess_mirror_env is believed to be confusing.)
#     """
#     return get_standard_mirror_env(envname)


def infer_repo_from_env(envname: Optional[EnvName]):
    if not envname:
        return None
    if is_cgap_env(envname):
        return 'cgap-portal'
    elif is_fourfront_env(envname):
        return 'fourfront'
    else:
        return None


# TODO: Figure out if these two actually designate different hosts or if we could make CGAP prettier. -kmp 4-Oct-2021
CGAP_FOURSIGHT_URL_PREFIX = "https://u9feld4va7.execute-api.us-east-1.amazonaws.com/api/view/"
FF_FOURSIGHT_URL_PREFIX = "https://foursight.4dnucleome.org/view/"


def infer_foursight_url_from_env(*, request=None, envname: Optional[EnvName] = None):
    token = infer_foursight_from_env(request=request, envname=envname)
    if token is not None:
        prefix = CGAP_FOURSIGHT_URL_PREFIX if is_cgap_env(envname) else FF_FOURSIGHT_URL_PREFIX
        return prefix + token
    else:
        return None


def infer_foursight_from_env(*, request=None, envname: Optional[EnvName] = None):
    """  Infers the Foursight environment to view based on the given envname and request context

    :param request: the current request (or an object that has member 'domain')
    :param envname: name of the environment we are on
    :return: Foursight env at the end of the url ie: for fourfront-green, could be either 'data' or 'staging'
    """
    if not envname or (not is_stg_or_prd_env(envname) and not envname.startswith('fourfront-')):
        return None
    elif is_cgap_env(envname):
        return short_env_name(envname)  # all cgap envs are prefixed and the FS envs directly match
    else:
        if is_stg_or_prd_env(envname):
            if FF_PUBLIC_DOMAIN_PRD in request.domain:
                return FF_PRODUCTION_IDENTIFIER
            else:
                return FF_STAGING_IDENTIFIER
        else:
            return short_env_name(envname)  # if not data/staging, behaves exactly like CGAP


def short_env_name(envname: Optional[EnvName]):
    """
    Given a short or long environment name, return the short name.
    For legacy systems, this implies that 'fourfront-' will be removed if present.
    For orchestrated systems in the future, this may remove some other prefix, subject to declarations.
    (In any case, this is the right way to shorten a name.
    Please do NOT use substrings based on len of some presumed prefix string.)

    Examples:
            short_env_name('cgapdev') => 'cgapdev'
            short_env_name('fourfront-cgapdev') => 'cgapdev'

    Args:
        envname str: the short or long name of a beanstalk environment

    Returns:
        a string that is the short name of the specified beanstalk environment
    """

    if not envname:
        return None
    elif '_' in envname:
        # e.g., "fourfront_mastertest" does not shorten.
        return envname
    elif envname.startswith('fourfront-'):
        # Note that EVEN FOR CGAP we do not look for 'cgap-' because this is the legacy implementation,
        # not the orchestrated implementation. We'll fix that problem elsewhere. Within the legacy
        # beanstalk environment, the short name of 'fourfront-cgapdev' is 'cgapdev' and not 'dev'.
        # Likewise, the long name of 'cgapdev' is 'fourfront-cgapdev', not 'cgap-dev' etc.
        # -kmp 4-Oct-2021
        return remove_prefix('fourfront-', envname)
    else:
        return envname


def full_env_name(envname: EnvName):
    """
    Given the possibly-short name of a Fourfront or CGAP beanstalk environment, return the long name.

    The short name is allowed to omit 'fourfront-' but the long name is not.

    Examples:
        full_env_name('cgapdev') => 'fourfront-cgapdev'
        full_env_name('fourfront-cgapdev') => 'fourfront-cgapdev'

    Args:
        envname str: the short or long name of a beanstalk environment

    Returns:
        a string that is the long name of the specified beanstalk environment
    """
    if envname in ('data', 'staging'):
        # This is problematic for Fourfront because of mirroring.
        # For cgap, it's OK for us to just return fourfront-cgap because there's no ambiguity.
        # Also, with 'data' and 'staging', you can tell it didn't come from fourfront-data and fourfront-staging,
        # whereas with 'cgap' it's harder to be sure it didn't start out 'fourfront-cgap' and get shortened,
        # so it's best not to get too fussy about 'cgap'. -kmp 4-Oct-2021
        raise ValueError("The special token '%s' is not a beanstalk environment name." % envname)
    elif '_' in envname:
        # e.g., "fourfront_mastertest" does not shorten.
        return envname
    elif not envname.startswith('fourfront-'):
        return 'fourfront-' + envname
    else:
        return envname


def full_cgap_env_name(envname: EnvName):
    check_true(isinstance(envname, str) and "cgap" in envname, "The envname is not a CGAP env name.",
               error_class=ValueError)
    return full_env_name(envname)


def full_fourfront_env_name(envname: EnvName):
    check_true(isinstance(envname, str) and "cgap" not in envname, "The envname is not a Fourfront env name.",
               error_class=ValueError)
    return full_env_name(envname)


def classify_server_url(url, raise_error=True):
    """
    Given a server url, returns a dictionary of information about how it relates to the Fourfront & CGAP ecosystem.

    If a useful result cannot be be computed, and raise_error is True, an error is raised.
    Otherwise, a three values are computed and returned as part of a single dictionary:

    * a "kind", which is 'fourfront', 'cgap', 'localhost', or (if raise_error is False) 'unknown'.
    * an "environment", which is the name of a fourfront or cgap environment (as appropriate) or 'unknown'.
    * a boolean "is_stg_or_prd" that is True if the environment is a production (staging or production) environment,
      and False otherwise.

    Parameters:

      url: a server url to be classified
      raise_error(bool): whether to raise an error if the classification is unknown

    Returns:

      a dictionary of information containing keys "kind", "environment", and "is_stg_or_prd"
    """

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


# I don't think this function is ever called in any repo. -kmp 15-May-2022
#
# def make_env_name_cfn_compatible(env_name: EnvName) -> str:
#     """ Common IDs in Cloudformation forbid the use of '-', and we don't want to change
#         our environment name formatting so this simple method is provided to document this
#         behavior. ex: cgap-mastertest -> cgapmastertest
#     """
#     return env_name.replace('-', '')
