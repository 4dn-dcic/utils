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


def find_valid_file_or_extra_file(obj_id, format):
    """
    Helper function. Get the file metadata for the given obj_id and display
    other file formats that could also be downloaded, if extra_files is present
    on the object.
    Returns a dict with the following keys: metadata, full_href, full_path
    """
    try:
        file_meta = get_metadata(obj_id)  # NOQA
    except Exception as exc:
        raise Exception('Could not open file: %s. Reason: %s' % (obj_id, exc))

    if ('upload_key' not in file_meta or 'href' not in file_meta or 'File' not in file_meta.get('@type', [])):
        raise Exception('Could not open file: %s. Reason: it is not a valid file object.' % obj_id)

    main_ff = file_meta['file_format']['display_title']
    all_ffs = {main_ff: {'uk': file_meta['upload_key'], 'href': file_meta['href']}}
    ff_display = main_ff + ' (default)\n'

    if len(file_meta.get('extra_files', [])) > 0:
        for extra_file in file_meta['extra_files']:
            if 'file_format' not in extra_file or 'upload_key' not in extra_file:
                continue
            extra_ff = extra_file['file_format']['display_title']
            all_ffs[extra_ff] = {'uk': extra_file['upload_key'], 'href': extra_file['href']}
            ff_display += (extra_ff + '\n')
        if format is None and len(all_ffs) > 1:  # notify the users of extra files
            print('There are extra files associated with the given file ID (%s). '
                  'The file formats of those files are:\n%sTo open an extra '
                  'file, add the following parameter: `format=<chosen format>`'
                  % (obj_id, ff_display))

    if format and format not in all_ffs:
        raise Exception('Could not open file: %s. Reason: invalid file format '
                        'supplied. Use one of the following:\n%s' % (obj_id, ff_display))
    use_ff = format if (format is not None) else main_ff
    # the full fourfront href for the download
    full_href = '/'.join([DATA_SERVER, all_ffs[use_ff]['href']])

    # get the full Jupyterhub path for the file
    home_dir = '/home/jovyan'
    # hardcoded so that we can find which volume to look in by file type
    # will need to be updated if more File types are added to proc file bucket
    if 'FileVistrack' in file_meta['@type'] or 'FileProcessed' in file_meta['@type']:
        data_dir = 'proc_data'
    else:
        data_dir = 'raw_data'
    full_path = '/'.join([home_dir, data_dir, all_ffs[use_ff]['uk']])
    return {'metadata': file_meta, 'full_href': full_href, 'full_path': full_path}


def locate_4dn_file(obj_id, format=None):
    """
    Use this function to return the full download href for a file on the
    Fourfront data portal. Handles finding of extra_files using the `format`
    parameter
    """
    file_info = find_valid_file_or_extra_file(obj_id, format)
    return file_info['full_href']


def mount_4dn_file(obj_id, format=None):
    """
    Use this function to return the full filepath to a 4dn file on the
    Jupyterhub filesystem.
    EVENTUALLY this function will actually mount the file, but for now it will
    just direct the user to the pre-mounted file. Actually do a FF metadata
    check as well, and handle finding of extra_files using the `format`
    parameter
    """
    file_info = find_valid_file_or_extra_file(obj_id, format)
    # for now, ensure the file exists. Will catch non-uploaded files
    if not os.path.isfile(file_info['full_path']):
        raise Exception('Could not open file: %s. Reason: file cannot be mounted.' % obj_id)
    return file_info['full_path']


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
    file_info = find_valid_file_or_extra_file(obj_id, format)
    # this will be the base case in the future...
    if not local:
        # this seems to handle both binary and non-binary files
        ff_file = use_urllib.urlopen(file_info['full_href'])
    else:
        ff_file = open(file_info['full_path'])
        # see if the file is binary (needs to opened with 'rb' mode)
        # try to read a line from the file; if it is read, reset with seek()
        try:
            ff_file.readline()
        except UnicodeDecodeError:
            ff_file = open(file_info['full_path'], 'rb')
        else:
            ff_file.seek(0)
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
