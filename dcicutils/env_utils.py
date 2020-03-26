FOURFRONT_STG_OR_PRD_TOKENS = ['webprod', 'blue', 'green']
FOURFRONT_STG_OR_PRD_NAMES = ['staging', 'stagging', 'data']
CGAP_STG_OR_PRD_TOKENS = []
CGAP_STG_OR_PRD_NAMES = ['fourfront-cgap', 'fourfront-cgap-green', 'fourfront-cgap-blue']


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
