FOURFRONT_STG_OR_PRD_TOKENS = ['webprod', 'blue', 'green']
FOURFRONT_STG_OR_PRD_NAMES = ['staging', 'stagging', 'data']
CGAP_STG_OR_PRD_TOKENS = []
CGAP_STG_OR_PRD_NAMES = ['fourfront-cgap', 'fourfront-cgap-green', 'fourfront-cgap-blue']


def is_cgap_env(envname):
    return 'cgap' in envname


def is_fourfront_env(envname):
    return 'cgap' not in envname


def is_stg_or_prd_env(envname):
    stg_or_prd_tokens = CGAP_STG_OR_PRD_TOKENS if is_cgap_env(envname) else FOURFRONT_STG_OR_PRD_TOKENS
    stg_or_prd_names = CGAP_STG_OR_PRD_NAMES if is_cgap_env(envname) else FOURFRONT_STG_OR_PRD_NAMES
    if envname in stg_or_prd_names:
        return True
    elif any(token in envname for token in stg_or_prd_tokens):
        return True
    return False
