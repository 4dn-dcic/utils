import functools
import types
import os
import sys
from contextlib import contextmanager
from dcicutils.ff_utils import *  # NOQA
# urllib is different between python2 and 3
if sys.version_info[0] == 3:
    import urllib.request as use_urllib
else:
    import urllib2 as use_urllib


# do some top level stuff when the module is imported
if 'FF_ACCESS_KEY' not in os.environ or 'FF_ACCESS_SECRET' not in os.environ:
    raise Exception('ERROR USING JUPYTERHUB_UTILS! FF_ACCESS_KEY and/or '
                    'FF_ACCESS_SECRET are not in the environment. Please'
                    ' check INIT_ERR_OUTPUT for notebook init errors.')

# allow this to be set through an environment variable, mainly for testing
# it's okay that users could theoretically change the server by altering this
# env variable, since all that would do is make these functions not work
DATA_SERVER = os.environ.get('_JH_FF_SERVER', 'https://data.4dnucleome.org')

# these are the functions that jupyterhub_authenticator_deco will be applied to
# this means that they will automatically work in Jupyterhub with setting keys
HANDLED_FUNCTIONS = [
    'get_metadata',
    'post_metadata',
    'patch_metadata',
    'upsert_metadata',
    'search_metadata',
    'delete_field'
]


class HTTPBasic403AuthHandler(use_urllib.HTTPBasicAuthHandler):
    """
    See: https://gist.github.com/dnozay/194d816aa6517dc67ca1
    retry with basic auth when facing a 403 forbidden
    Needed because using basic auth headers wasn't working with urllib
    """
    #
    def http_error_403(self, req, fp, code, msg, headers):
        host = req.host
        realm = None
        return self.retry_http_basic_auth(host, req, realm)


def install_auth_opener():
    '''
    See: https://gist.github.com/dnozay/194d816aa6517dc67ca1
    install the authentication handler.
    This handles non-standard behavior where the server responds with
    403 forbidden, instead of 401 retry. Which means it does not give you the
    chance to provide your credentials.'''
    # create opener / 403 error handler
    auth_handler = HTTPBasic403AuthHandler()
    # install it.
    opener = use_urllib.build_opener(auth_handler)
    use_urllib.install_opener(opener)
    return auth_handler


def add_password(auth_handler, uri, username, passwd, realm=None):
    '''
    See: https://gist.github.com/dnozay/194d816aa6517dc67ca1
    add credentials for basic auth to the password manager
    '''
    auth_handler.add_password(realm=realm, uri=uri, user=username,
                              passwd=passwd)


def jupyterhub_authenticator_deco(fxn):
    @functools.wraps(fxn)
    def wrapper(*args, **kwargs):
        """
        Wrapper function that reads access key ID and secret from the environment
        variables initialized when starting up a single-user Jupyter Notebook
        on the Fourfront Jupyterhub
        """
        if 'ff_env' in kwargs:
            del kwargs['ff_env']
        # key needs server information. Make it always data.4dnucleome for now
        if 'key' not in kwargs or not (isinstance(kwargs['key'], dict) and
                                       {'key', 'secret', 'server'} <= set(kwargs['key'].keys())):
            kwargs['key'] = {'key': os.environ['FF_ACCESS_KEY'],
                             'secret': os.environ['FF_ACCESS_SECRET'],
                             'server': DATA_SERVER}
        return fxn(*args, **kwargs)
    return wrapper


def automatically_decorate_handled_functions():
    """
    This is a bit hacky, but saves a lot of boilerplate code.
    Iterate through function names in HANDLED_FUNCTIONS and, if the object
    exists in ff_utils and is actually a function, then apply the
    jupyterhub_authenticator_deco to it and overwrite in the current module
    """
    this_module = sys.modules[__name__]
    for fxn_name in HANDLED_FUNCTIONS:
        fxn_obj = getattr(this_module, fxn_name)
        # ensure it's actually a function
        if isinstance(fxn_obj, types.FunctionType):
            # set the value
            setattr(this_module, fxn_name, jupyterhub_authenticator_deco(fxn_obj))


@contextmanager
def open_4dn_file(obj_id, format=None, local=True):
    """
    Use this function to open a 4dn file, given an object id. Usage:
        with open_4dn_file(<object id>) as f:
            ...
    Under the hood, this file makes a request to Fourfront and obtains
    the download url for the file. If permissions do not match, an exception
    will be thrown at that point. A context manager is used to open the file
    like a regular Python file object.
    obj_id can be any identifying object property (accession, uuid, @id...)

    If there are extra files associated with the given file, a message will be
    printed explaining how to access them using this function. This is done
    by using the `format` parameter. It can be set to the file format of any of
    the extra files (or the original file). None means the original file is used

    Temporarily, the `local` parameter is used to redirect file opening to the
    local JH volume where goofys is mounted. Setting local to False will cause
    urllib to be open. Eventually, this will be the default behavior.
    """
    try:
        file_meta = get_metadata(obj_id)  # NOQA
    except Exception as exc:
        raise Exception('Could not open file: %s. Reason: %s' % (obj_id, exc))

    if ('upload_key' not in file_meta or 'href' not in file_meta or 'File' not in file_meta.get('@type', [])):
        raise Exception('Could not open file: %s. Reason: it is not a valid file object.' % obj_id)

    main_ff = file_meta['file_format']['display_title']
    all_ffs = {main_ff: {'uk': file_meta['upload_key'], 'href': file_meta['href']}}

    if len(file_meta.get('extra_files', [])) > 0:
        ff_display = main_ff + ' (default)\n'
        for extra_file in file_meta['extra_files']:
            if 'file_format' not in extra_file or 'upload_key' not in extra_file:
                continue
            extra_ff = extra_file['file_format']['display_title']
            all_ffs[extra_ff] = {'uk': extra_file['upload_key'], 'href': extra_file['href']}
            ff_display += (extra_ff + '\n')
        if format is None and len(all_ffs) > 1:  # notify the users of extra files
            print('There are extra files associated with the given file ID (%s). '
                  'The file formats of those files are:\n%sTo open an extra '
                  'file, use `open_4dn_file(%s, format=<chosen format>)`'
                  % (obj_id, ff_display, obj_id))

    if format and format not in all_ffs:
        raise Exception('Could not open file: %s. Reason: invalid file format '
                        'supplied. Use one of the following:\n%s' % (obj_id, ff_display))
    use_ff = format if (format is not None) else main_ff
    file_path = all_ffs[use_ff]['uk']
    file_href = all_ffs[use_ff]['href']

    if not local:  # this will be the base case in the future...
        full_href = '/'.join([DATA_SERVER, file_href])
        ff_file = use_urllib.urlopen(full_href)
    else:
        home_dir = '/home/jovyan'
        # hardcoded so that we can find which volume to look in by file type
        if 'FileVistrack' in file_meta['@type'] or 'FileProcessed' in file_meta['@type']:
            data_dir = 'proc_data'
        else:
            data_dir = 'raw_data'
        jh_path = '/'.join([home_dir, data_dir, file_path])
        ff_file = open(jh_path)
    try:
        yield ff_file
    finally:
        ff_file.close()


# LASTLY, do setup that requires the above functions to be defined
# apply jupyterhub_authenticator_deco to all fxns in HANDLED_FUNCTIONS
automatically_decorate_handled_functions()

# set up authorization handling for urllib
AUTH_HANDLER = install_auth_opener()
add_password(AUTH_HANDLER, DATA_SERVER, os.environ['FF_ACCESS_KEY'],
             os.environ['FF_ACCESS_SECRET'])
