import functools
import types
import os
import sys
from dcicutils.ff_utils import *

# these are the functions that jupyterhub_authenticator_deco will be applied to
# this means that they will automatically work in Jupyterhub with setting keys
handled_functions = [
    'get_metadata',
    'post_metadata',
    'patch_metadata',
    'upsert_metadata',
    'search_metadata',
    'delete_field'
]

def jupyterhub_authenticator_deco(fxn):
    @functools.wraps(fxn)
    def wrapper(*args, **kwargs):
        """
        Wrapper function that reads access key ID and secret from the environment
        variables initialized when starting up a single-user Jupyter Notebook
        on the Fourfront Jupyterhub
        """
        if 'FF_ACCESS_KEY' not in os.environ or 'FF_ACCESS_SECRET' not in os.environ:
            raise Exception('ERROR USING JUPYTERHUB_UTILS! FF_ACCESS_KEY and/or '
                            'FF_ACCESS_SECRET are not in the environment. Please'
                            ' check INIT_ERR_OUTPUT for notebook init errors.')
        if 'ff_env' in kwargs:
            del kwargs['ff_env']
        # key needs server information. Make it always data.4dnucleome for now
        if 'key' not in kwargs or not (isinstance(kwargs['key'], dict) and
            {'key', 'secret', 'server'} <= set(kwargs['key'].keys())):
            kwargs['key'] = {'key': os.environ['FF_ACCESS_KEY'],
                             'secret': os.environ['FF_ACCESS_SECRET'],
                             'server': 'https://data.4dnucleome.org/'}
        return fxn(*args, **kwargs)
    return wrapper


def automatically_decorate_handled_functions():
    """
    This is a bit hacky, but saves a lot of boilerplate code.
    Iterate through all objects in ff_utils and, if the object is a function
    and it contained in handled_functions (by obj.__name__), then apply the
    jupyterhub_authenticator_deco to it and overwrite in the current module
    """
    this_module = sys.modules[__name__]
    for fxn_name in handled_functions:
        fxn_obj = getattr(this_module, fxn_name)
        # ensure it's actually a function
        if isinstance(fxn_obj, types.FunctionType):
            # set the value
            setattr(this_module, fxn_name, jupyterhub_authenticator_deco(fxn_obj))

# call it
automatically_decorate_handled_functions()
