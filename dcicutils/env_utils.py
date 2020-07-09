import os
from .misc_utils import get_setting_from_context


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
    'fourfront-cgaptest': 'test',
    'fourfront-cgapwolf': 'test',

    'cgap-dev': 'test',
    'cgap-test': 'test',
    'cgap-wolf': 'test',

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
