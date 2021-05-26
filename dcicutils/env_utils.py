import os

from .misc_utils import get_setting_from_context, check_true
from urllib.parse import urlparse


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

CGAP_PUBLIC_URL_STG = 'https://staging.cgap.hms.harvard.edu'  # This is a stopgap for testing and may have to change
CGAP_PUBLIC_URL_PRD = 'https://cgap.hms.harvard.edu'

CGAP_PUBLIC_URLS = {
    'cgap': CGAP_PUBLIC_URL_PRD,
    'data': CGAP_PUBLIC_URL_PRD,
    'staging': CGAP_PUBLIC_URL_STG,
}

BEANSTALK_PROD_BUCKET_ENVS = {
    'staging': FF_PROD_BUCKET_ENV,
    'data': FF_PROD_BUCKET_ENV,
    FF_ENV_WEBPROD: FF_PROD_BUCKET_ENV,
    FF_ENV_WEBPROD2: FF_PROD_BUCKET_ENV,
    FF_ENV_PRODUCTION_BLUE: FF_PROD_BUCKET_ENV,
    FF_ENV_PRODUCTION_GREEN: FF_PROD_BUCKET_ENV,
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


def is_indexer_env(envname):
    """ Checks whether envname is an indexer environment.

    :param envname:  envname to check
    :return: True if envname is an indexer application, False otherwise
    """
    return envname in [FF_ENV_INDEXER, CGAP_ENV_INDEXER]


def indexer_env_for_env(envname):
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


def data_set_for_env(envname, default=None):
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


def blue_green_mirror_env(envname):
    """
    Given a blue envname, returns its green counterpart, or vice versa.
    For other envnames that aren't blue/green participants, this returns None.
    """
    if 'blue' in envname:
        return envname.replace('blue', 'green')
    elif 'green' in envname:
        return envname.replace('green', 'blue')
    else:
        return None


def prod_bucket_env(envname):
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


def get_bucket_env(envname):
    return prod_bucket_env(envname) if is_stg_or_prd_env(envname) else envname


def public_url_mappings(envname):
    """
    Returns a table of the public URLs we use for the ecosystem in which the envname resides.
    For example, if envname is a CGAP URL, this returns a set table of CGAP public URLs,
    and otherwise it returns a set of Fourfront URLs.

    The envname may be 'cgap', 'data', 'staging', or an environment name.
    """
    return CGAP_PUBLIC_URLS if is_cgap_env(envname) else FF_PUBLIC_URLS


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


def is_cgap_env(envname):
    """
    Returns True of the given string looks like a CGAP elasticbeanstalk environment name.
    Otherwise returns False.
    """
    return 'cgap' in envname if envname else False


def is_fourfront_env(envname):
    """
    Returns True of the given string looks like a Fourfront elasticbeanstalk environment name.
    Otherwise returns False.
    """
    return ('fourfront' in envname and 'cgap' not in envname) if envname else False


def is_stg_or_prd_env(envname):
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


def is_test_env(envname):
    return envname in BEANSTALK_TEST_ENVS if envname else False


def is_hotseat_env(envname):
    return 'hot' in envname if envname else False


# TODO: This variable and all the 'allow_environ=' arguments could go away in the next major version release.
#       --Kent & Will 15-Apr-2020
ALLOW_ENVIRON_BY_DEFAULT = True


def get_env_from_context(settings, allow_environ=ALLOW_ENVIRON_BY_DEFAULT):
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
        return guess_mirror_env(who_i_am)
    else:
        return None


def get_standard_mirror_env(envname):
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


def guess_mirror_env(envname):
    """
    Deprecated. This function returns what get_standard_mirror_env(envname) returns.
    (The name guess_mirror_env is believed to be confusing.)
    """
    return get_standard_mirror_env(envname)


def infer_repo_from_env(envname):
    if not envname:
        return None
    if is_cgap_env(envname):
        return 'cgap-portal'
    elif is_fourfront_env(envname):
        return 'fourfront'
    else:
        return None


def infer_foursight_from_env(request, envname):
    """  Infers the Foursight environment to view based on the given envname and request context

    :param request: the current request (or an object that has member 'domain')
    :param envname: name of the environment we are on
    :return: Foursight env at the end of the url ie: for fourfront-green, could be either 'data' or 'staging'
    """
    if is_cgap_env(envname):
        return envname[len('fourfront-'):]  # all cgap envs are prefixed and the FS envs directly match
    else:
        if is_stg_or_prd_env(envname):
            if FF_PUBLIC_DOMAIN_PRD in request.domain:
                return FF_PRODUCTION_IDENTIFIER
            else:
                return FF_STAGING_IDENTIFIER
        else:
            return envname[len('fourfront-'):]  # if not data/staging, behaves exactly like CGAP


def full_env_name(envname):
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
        raise ValueError("The special token '%s' is not a beanstalk environment name." % envname)
    elif not envname.startswith('fourfront-'):
        return 'fourfront-' + envname
    else:
        return envname


def full_cgap_env_name(envname):
    check_true(isinstance(envname, str) and "cgap" in envname, "The envname is not a CGAP env name.",
               error_class=ValueError)
    return full_env_name(envname)


def full_fourfront_env_name(envname):
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


def make_env_name_cfn_compatible(env_name: str) -> str:
    """ Common IDs in Cloudformation forbid the use of '-', and we don't want to change
        our environment name formatting so this simple method is provided to document this
        behavior. ex: cgap-mastertest -> cgapmastertest
    """
    return env_name.replace('-', '')
