import os


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


# Done this way to get maximally compatible behavior.
FOURFRONT_STG_OR_PRD_TOKENS = ['webprod', 'blue', 'green']
FOURFRONT_STG_OR_PRD_NAMES = ['staging', 'stagging', 'data']

# Done this way because it's safer going forward.
CGAP_STG_OR_PRD_TOKENS = []
CGAP_STG_OR_PRD_NAMES = [CGAP_ENV_WEBPROD, CGAP_ENV_PRODUCTION_GREEN, CGAP_ENV_PRODUCTION_BLUE]


# These operate as pairs. Don't add extras.
BEANSTALK_PROD_MIRRORS = {

    FF_ENV_PRODUCTION_BLUE: FF_ENV_PRODUCTION_GREEN,
    FF_ENV_PRODUCTION_GREEN: FF_ENV_PRODUCTION_BLUE,
    FF_ENV_WEBPROD: FF_ENV_WEBPROD2,
    FF_ENV_WEBPROD2: FF_ENV_WEBPROD,

    CGAP_ENV_PRODUCTION_BLUE: CGAP_ENV_PRODUCTION_GREEN,
    CGAP_ENV_PRODUCTION_GREEN: CGAP_ENV_PRODUCTION_BLUE,
    CGAP_ENV_WEBPROD: None,

    CGAP_ENV_PRODUCTION_BLUE_NEW: CGAP_ENV_PRODUCTION_GREEN_NEW,
    CGAP_ENV_PRODUCTION_GREEN_NEW: CGAP_ENV_PRODUCTION_BLUE_NEW,

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


def is_cgap_env(envname):
    """
    Returns True of the given string looks like a CGAP elasticbeanstalk environment name.
    Otherwise returns False.
    """
    return 'cgap' in envname


def is_fourfront_env(envname):
    """
    Returns True of the given string looks like a Fourfront elasticbeanstalk environment name.
    Otherwise returns False.
    """
    return 'cgap' not in envname


def is_stg_or_prd_env(envname):
    """
    Returns True if the given envname is the name of something that might be either live data or something
    that is ready to be swapped in as live.  So things like 'staging' and either of 'blue/green' will return
    True whether or not they are actually the currently live instance. (This function doesn't change its
    state as blue or green is deployed, in other words.)
    """
    stg_or_prd_tokens = CGAP_STG_OR_PRD_TOKENS if is_cgap_env(envname) else FOURFRONT_STG_OR_PRD_TOKENS
    stg_or_prd_names = CGAP_STG_OR_PRD_NAMES if is_cgap_env(envname) else FOURFRONT_STG_OR_PRD_NAMES
    if envname in stg_or_prd_names:
        return True
    elif any(token in envname for token in stg_or_prd_tokens):
        return True
    return False


def is_test_env(envname):
    return envname in BEANSTALK_TEST_ENVS


def is_hotseat_env(envname):
    return 'hot' in envname


ALLOW_ENVIRON_BY_DEFAULT = True


def get_env_from_context(settings, allow_environ=ALLOW_ENVIRON_BY_DEFAULT):
    if allow_environ:
        environ_env_name = os.environ.get('ENV_NAME')
        if environ_env_name:
            return environ_env_name
    return settings.get('env.name')


def get_mirror_env_from_context(settings, allow_environ=ALLOW_ENVIRON_BY_DEFAULT, allow_guess=True, ):
    # TODO: I added allow_environ featurism here but did not yet enable it.
    #       Want to talk to Will about whether we should consider that a compatible or breaking hcange.
    #       -kmp 27-Mar-2020
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


def guess_mirror_env(envname):
    # TODO: Should this be BEANSTALK_PROD_MIRRORS.get(envname) or blue_green_mirror_env(envname)
    return BEANSTALK_PROD_MIRRORS.get(envname)
