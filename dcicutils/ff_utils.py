from __future__ import print_function
import json
import time
import random
import copy
import boto3
from uuid import UUID
from dcicutils import s3_utils, submit_utils
import requests


HIGLASS_BUCKETS = ['elasticbeanstalk-fourfront-webprod-wfoutput',
                   'elasticbeanstalk-fourfront-webdev-wfoutput']


##################################
# Widely used metadata functions #
##################################

def standard_request_with_retries(request_fxn, url, auth, verb, **kwargs):
    """
    Standard function to execute the request made by authorized_request.
    If desired, you can write your own retry handling, but make sure
    the arguments are formatted identically to this function.
    request_fxn is the request function, url is the string url,
    auth is the tuple standard authentication, and verb is the string
    kind of verb. any additional kwargs are passed to the request.
    Handles errors and returns the response if it has a status
    code under 400.
    """
    # execute with retries, if necessary
    final_res = None
    error = None
    retry = 0
    non_retry_statuses = [401, 402, 403, 404, 405, 422]
    retry_timeouts = [0, 1, 2, 3, 4]
    while final_res is None and retry < len(retry_timeouts):
        time.sleep(retry_timeouts[retry])
        try:
            res = request_fxn(url, auth=auth, **kwargs)
        except Exception as e:
            retry += 1
            error = 'Error with %s request for %s: %s' % (verb.upper(), url, e)
            continue
        if res.status_code >= 400:
            # attempt to get reason from res.json. then try raise_for_status
            try:
                err_reason = res.json()
            except ValueError:
                try:
                    res.raise_for_status()
                except Exception as e:
                    err_reason = repr(e)
                else:
                    err_reason = res.reason
            retry += 1
            error = ('Bad status code for %s request for %s: %s. Reason: %s'
                     % (verb.upper(), url, res.status_code, err_reason))
            if res.status_code in non_retry_statuses:
                break
        else:
            final_res = res
            error = None
    if error and not final_res:
        raise Exception(error)
    return final_res


def search_request_with_retries(request_fxn, url, auth, verb, **kwargs):
    """
    Example of using a non-standard retry function. This one is for searches,
    which return a 404 on an empty search. Handle this case so an empty array
    is returned as a search result and not an error
    """
    final_res = None
    error = None
    retry = 0
    # include 400 here because it is returned for invalid search types
    non_retry_statuses = [400, 401, 402, 403, 404, 405, 422]
    retry_timeouts = [0, 1, 2, 3, 4]
    while final_res is None and retry < len(retry_timeouts):
        time.sleep(retry_timeouts[retry])
        try:
            res = request_fxn(url, auth=auth, **kwargs)
        except Exception as e:
            retry += 1
            error = 'Error with %s request for %s: %s' % (verb.upper(), url, e)
            continue
        # look for a json response with '@graph' key
        try:
            res_json = res.json()
        except ValueError:
            res_json = {}
            try:
                res.raise_for_status()
            except Exception as e:
                err_reason = repr(e)
            else:
                err_reason = res.reason
            retry += 1
            error = ('Bad status code for %s request for %s: %s. Reason: %s'
                     % (verb.upper(), url, res.status_code, err_reason))
        else:
            if res_json.get('@graph') is not None:
                final_res = res
                error = None
            else:
                retry += 1
                error = ('Bad status code for %s request for %s: %s. Reason: %s'
                         % (verb.upper(), url, res.status_code, res_json))
        if res.status_code in non_retry_statuses:
            break
    if error and not final_res:
        raise Exception(error)
    return final_res


def authorized_request(url, auth=None, ff_env=None, verb='GET',
                       retry_fxn=standard_request_with_retries, **kwargs):
    """
    Generalized function that handles authentication for any type of request to FF.
    Takes a required url, request verb, auth, fourfront environment, and optional
    retry function and headers. Any other kwargs provided are also past into the request.
    For example, provide a body to a request using the 'data' kwarg.
    Timeout of 60 seconds used by default but can be overwritten as a kwarg.

    Verb should be one of: GET, POST, PATCH, PUT, or DELETE
    auth should be obtained using s3Utils.get_key or in submit_utils tuple form.
    If not provided, try to get the key using s3_utils if 'ff_env' in kwargs

    usage:
    authorized_request('https://data.4dnucleome.org/<some path>', (authId, authSecret))
    OR
    authorized_request('https://data.4dnucleome.org/<some path>', ff_env='fourfront-webprod')
    """
    use_auth = unified_authentication(auth, ff_env)
    headers = kwargs.get('headers')
    if not headers:
        kwargs['headers'] = {'content-type': 'application/json', 'accept': 'application/json'}
    if 'timeout' not in kwargs:
        kwargs['timeout'] = 60  # default timeout

    verbs = {'GET': requests.get,
             'POST': requests.post,
             'PATCH': requests.patch,
             'PUT': requests.put,
             'DELETE': requests.delete,
             }
    try:
        the_verb = verbs[verb.upper()]
    except KeyError:
        raise Exception("Provided verb %s is not valid. Must one of: %s" % (verb.upper(), ', '.join(verbs.keys())))
    # automatically detect a search and overwrite the retry if it is standard
    if '/search/' in url and retry_fxn == standard_request_with_retries:
        retry_fxn = search_request_with_retries
    # use the given retry function. MUST TAKE THESE PARAMS!
    return retry_fxn(the_verb, url, use_auth, verb, **kwargs)


def get_metadata(obj_id, key=None, ff_env=None, frame="embedded", check_queue=False):
    """
    Function to get metadata for a given obj_id (uuid or @id, most likely).
    Either takes a dictionary form authentication (MUST include 'server')
    or a string fourfront-environment.
    Also takes a frame for the GET and a boolean 'check_queue', which if True
    will use information from the queues and/or datastore=database to
    ensure that the metadata is accurate.
    *REQUIRES ff_env if check_queue is used.*
    """
    auth = get_authentication_with_server(key, ff_env)
    get_url = '/'.join([auth['server'], obj_id, '?frame=' + frame])
    # check the queues if check_queue is True
    if check_queue and not stuff_in_queues(ff_env, check_secondary=False):
        get_url += '&datastore=database'
    response = authorized_request(get_url, auth=auth, verb='GET')
    return get_response_json(response)


def patch_metadata(patch_item, obj_id='', key=None, ff_env=None):
    '''
    Patch metadata given the patch body and an optional obj_id (if not provided,
    will attempt to use accession or uuid from patch_item body).
    Either takes a dictionary form authentication (MUST include 'server')
    or a string fourfront-environment.
    '''
    auth = get_authentication_with_server(key, ff_env)
    obj_id = obj_id if obj_id else patch_item.get('accession', patch_item.get('uuid'))
    if not obj_id:
        raise Exception("ERROR getting id from given object %s for the request to"
                        " patch item. Supply a uuid or accession." % obj_id)
    patch_url = '/'.join([auth['server'], obj_id])
    # format item to json
    patch_item = json.dumps(patch_item)
    response = authorized_request(patch_url, auth=auth, verb='PATCH', data=patch_item)
    return get_response_json(response)


def post_metadata(post_item, schema_name, key=None, ff_env=None, add_on=''):
    '''
    Patch metadata given the post body and a string schema name.
    Either takes a dictionary form authentication (MUST include 'server')
    or a string fourfront-environment.
    This function checks to see if an existing object already exists
    with the same body, and if so, runs a patch instead.
    add_on is the string that will be appended to the post url (used
    with tibanna)
    '''
    auth = get_authentication_with_server(key, ff_env)
    post_url = '/'.join([auth['server'], schema_name])
    post_url += add_on
    # format item to json
    post_item = json.dumps(post_item)
    try:
        response = authorized_request(post_url, auth=auth, verb='POST', data=post_item)
    except Exception as e:
        # this means there was a conflict. try to patch
        if '409' in str(e):
            return patch_metadata(json.loads(post_item), key=auth)
        else:
            raise Exception(str(e))
    return get_response_json(response)


def search_metadata(search, key=None, ff_env=None):
    """
    Make a get request of form <server>/<search> and returns the '@graph'
    key from the request json.
    Either takes a dictionary form authentication (MUST include 'server')
    or a string fourfront-environment.
    """
    auth = get_authentication_with_server(key, ff_env)
    if search.startswith('/'):
        search = search[1:]
    search_url = '/'.join([auth['server'], search])
    # use a different retry_fxn, since empty searches are returned as 400's
    response = authorized_request(search_url, auth=key, ff_env=ff_env,
                                  retry_fxn=search_request_with_retries)
    try:
        return get_response_json(response)['@graph']
    except KeyError as e:
        raise('Cannot get "@graph" from the search request for %s. Response '
              'status code is %s.' % (search_url, response.status_code))


def delete_field(obj_id, del_field, key=None, ff_env=None):
    """
    Given string obj_id and string del_field, delete a field(or fields seperated
    by commas). To support the old syntax, obj_id may be a dict item.
    Same auth mechanism as the other metadata functions
    """
    auth = get_authentication_with_server(key, ff_env)
    if isinstance(obj_id, dict):
        obj_id = obj_id.get("accession", obj_id.get("uuid"))
        if not obj_id:
            raise Exception("ERROR getting id from given object %s for the request to"
                            " delete field(s): %s. Supply a uuid or accession."
                            % (obj_id, del_field))
    delete_str = '?delete_fields=%s' % del_field
    patch_url = '/'.join([auth['server'], obj_id, delete_str])
    # use an empty patch body
    response = authorized_request(patch_url, auth=auth, verb='PATCH', data=json.dumps({}))
    return get_response_json(response)


#####################
# Utility functions #
#####################


def fdn_connection(key='', connection=None, keyname='default'):
    """
    This is a wrapper for getting submit_utils.FDN_Connection
    It's utility has decreased after transitioning to authorized_request
    """
    try:
        assert key or connection
    except AssertionError:
        return None
    if not connection:
        try:
            fdn_key = submit_utils.FDN_Key(key, keyname)
            connection = submit_utils.FDN_Connection(fdn_key)
        except Exception as e:
            raise Exception("Unable to connect to server with check keys : %s" % e)
    return connection


def unified_authentication(auth, ff_env):
    """
    One authentication function to rule them all.
    Has several options for authentication, which are:
    - manually provided tuple auth key (pass to key param)
    - manually provided dict key, like output of
      s3Utils.get_access_keys() (pass to key param)
    - string name of the fourfront environment (pass to ff_env param)
    (They are checked in this order).
    Handles errors for authentication and returns the tuple key to
    use with your request.
    """
    # first see if key should be obtained from using ff_env
    if not auth and ff_env:
        # webprod and webprod2 both use the fourfront-webprod bucket for keys
        use_env = 'fourfront-webprod' if 'webprod' in ff_env else ff_env
        auth = s3_utils.s3Utils(env=use_env).get_access_keys()
    # see if auth is directly from get_access_keys() or the tuple form used in submit_utils
    use_auth = None
    # needed for old form of auth from get_key()
    if isinstance(auth, dict) and isinstance(auth.get('default'), dict):
        auth = auth['default']
    if isinstance(auth, dict) and 'key' in auth and 'secret' in auth:
        use_auth = (auth['key'], auth['secret'])
    elif isinstance(auth, tuple) and len(auth) == 2:
        use_auth = auth
    if not use_auth:
        raise Exception("Must provide a valid authorization key or ff "
                        "environment. You gave: %s (key), %s (ff_env)" % (auth, ff_env))
    return use_auth


def get_authentication_with_server(auth, ff_env):
    """
    Pass in authentication information and ff_env and attempts to either
    retrieve the server info from the auth, or if it cannot, get the
    key with s3_utils given
    """
    if isinstance(auth, dict) and isinstance(auth.get('default'), dict):
        auth = auth['default']
    # if auth does not contain the 'server', we must get fetch from s3
    if not isinstance(auth, dict) or not {'key', 'secret', 'server'} <= set(auth.keys()):
        # must have ff_env if we want to get the key
        if not ff_env:
            raise Exception("ERROR GETTING SERVER!\nMust provide dictionary auth with"
                            " 'server' or ff environment. You gave: %s (auth), %s (ff_env)"
                            % (auth, ff_env))
        auth = s3_utils.s3Utils(env=ff_env).get_access_keys()
        if 'server' not in auth:
            raise Exception("ERROR GETTING SERVER!\nAuthentication retrieved using "
                            " ff environment does not have server information. Found: %s (auth)"
                            ", %s (ff_env)" % (auth, ff_env))
    # ensure that the server does not end with '/'
    if auth['server'].endswith('/'):
        auth['server'] = auth['server'][:-1]
    return auth


def stuff_in_queues(ff_env, check_secondary=False):
    """
    Used to guarantee up-to-date metadata by checking the contents of the indexer queues.
    If items are currently waiting in the primary or deferred queues, return False.
    If check_secondary is True, will also require the secondary queue.
    """
    if not ff_env:
        raise Exception("Must provide a full fourfront environment name to "
                        "this function (such as 'fourfront-webdev'). You gave: "
                        "%s" % ff_env)
    empty_queues = False
    client = boto3.client('sqs', region_name='us-east-1')
    queue_names = ['-indexer-queue', '-deferred-indexer-queue']
    if check_secondary:
        queue_names.append('-secondary-indexer-queue')
    for queue_name in queue_names:
        try:
            queue_url = client.get_queue_url(
                QueueName=ff_env + queue_name
            ).get('QueueUrl')
            queue_attrs = client.get_queue_attributes(
                QueueUrl=queue_url,
                AttributeNames=['ApproximateNumberOfMessages', 'ApproximateNumberOfMessagesNotVisible']
            ).get('Attributes', {})
        except Exception as e:
            print('Error finding queue or its attributes: %s' % ff_env + queue_name)
            empty_queues = False  # queue not found. use datastore=database
            break
        else:
            visible = queue_attrs.get('ApproximateNumberOfMessages', '-1')
            not_vis = queue_attrs.get('ApproximateNumberOfMessagesNotVisible', '-1')
            if (visible and int(visible) == 0) and (not_vis and int(not_vis) == 0):
                empty_queues = True
            else:
                empty_queues = False
                break
    return empty_queues


def get_response_json(res):
    """
    Very simple function to return json from a response or raise an error if
    it is not present. Used with the metadata functions.
    """
    res_json = None
    try:
        res_json = res.json()
    except Exception as e:
        raise Exception('Cannot get json for request to %s. Status'
                        ' code: %s. Response text: %s' %
                        (res.url, res.status_code, res.text))
    return res_json


def convert_param(parameter_dict, vals_as_string=False):
    '''
    converts dictionary format {argument_name: value, argument_name: value, ...}
    to {'workflow_argument_name': argument_name, 'value': value}
    '''
    print(str(parameter_dict))
    metadata_parameters = []
    for k, v in parameter_dict.items():
        # we need this to be a float or integer if it really is, else a string
        if not vals_as_string:
            try:
                v = float(v)
                if v % 1 == 0:
                    v = int(v)
            except ValueError:
                v = str(v)
        else:
            v = str(v)

        metadata_parameters.append({"workflow_argument_name": k, "value": v})

    print(str(metadata_parameters))
    return metadata_parameters


def generate_rand_accession():
    rand_accession = ''
    for i in range(7):
        r = random.choice('ABCDEFGHIJKLMNOPQRSTUVWXYZ123456789')
        rand_accession += r
    accession = "4DNFI"+rand_accession
    return accession


def is_uuid(value):
    """Does the string look like a uuid"""
    if '-' not in value:
        # md5checksums are valid uuids but do not contain dashes so this skips those
        return False
    try:
        UUID(value, version=4)
        return True
    except ValueError:  # noqa: E722
        return False


def find_uuids(val):
    """Find any uuids in the value"""
    vals = []
    if not val:
        return []
    elif isinstance(val, str):
        if is_uuid(val):
            vals = [val]
        else:
            return []
    else:
        text = str(val)
        text_list = [i for i in text. split("'") if len(i) == 36]
        vals = [i for i in text_list if is_uuid(i)]
    return vals


def get_item_type(connection, item):
    try:
        return item['@type'].pop(0)
    except (KeyError, TypeError):
        res = submit_utils.get_FDN(item, connection)
        try:
            return res['@type'][0]
        except AttributeError:  # noqa: E722
            print("Can't find a type for item %s" % item)
    return None


def filter_dict_by_value(dictionary, values, include=True):
    """Will filter items from a dictionary based on values
        can be either an inclusive or exclusive filter
        if include=False will remove the items with given values
        else will remove items that don't match the given values
    """
    if include:
        return {k: v for k, v in dictionary.items() if v in values}
    else:
        return {k: v for k, v in dictionary.items() if v not in values}


def has_field_value(item_dict, field, value=None, val_is_item=False):
    """Returns True if the field is present in the item
        BUT if there is value parameter only returns True if value provided is
        the field value or one of the values if the field is an array
        How fancy do we want to make this?"""
    # 2 simple cases
    if field not in item_dict:
        return False
    if not value and field in item_dict:
        return True

    # now checking value
    val_in_item = item_dict.get(field)

    if isinstance(val_in_item, list):
        if value in val_in_item:
            return True
    elif isinstance(val_in_item, str):
        if value == val_in_item:
            return True

    # only check dict val_is_item param is True and only
    # check @id and link_id - uuid raw format will have been
    # checked above
    if val_in_item:
        if isinstance(val_in_item, dict):
            ids = [val_in_item.get('@id'), val_in_item.get('link_id')]
            if value in ids:
                return True
    return False


def get_types_that_can_have_field(connection, field):
    """find items that have the passed in fieldname in their properties
        even if there is currently no value for that field"""
    profiles = submit_utils.get_FDN('/profiles/', connection=connection, frame='raw')
    types_w_field = []
    for t, j in profiles.items():
        if j['properties'].get(field):
            types_w_field.append(t)
    return types_w_field


def get_linked_items(connection, itemid, found_items={},
                     no_children=['Publication', 'Lab', 'User', 'Award']):
    """Given an ID for an item all descendant linked item uuids (as given in 'frame=raw')
        are stored in a dict with each item type as the value.
        All descendants are retrieved recursively except the children of the types indicated
        in the no_children argument.
        The relationships between descendant linked items are not preserved - i.e. you don't
        know who are children, grandchildren, great grandchildren ... """
    if not found_items.get(itemid):
        res = submit_utils.get_FDN(itemid, connection=connection, frame='raw')
        if 'error' not in res['status']:
            # create an entry for this item in found_items
            try:
                obj_type = submit_utils.get_FDN(itemid, connection=connection)['@type'][0]
                found_items[itemid] = obj_type
            except AttributeError:  # noqa: E722
                print("Can't find a type for item %s" % itemid)
            if obj_type not in no_children:
                fields_to_check = copy.deepcopy(res)
                id_list = []
                for key, val in fields_to_check.items():
                    # could be more than one item in a value
                    foundids = find_uuids(val)
                    if foundids:
                        id_list.extend(foundids)
                if id_list:
                    id_list = [i for i in list(set(id_list)) if i not in found_items]
                    for uid in id_list:
                        found_items.update(get_linked_items(connection, uid, found_items))
    return found_items
