import boto3
import json
import os
import random
import requests
import time

from collections import namedtuple
from elasticsearch.exceptions import AuthorizationException
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse
from . import (
    s3_utils,
    es_utils,
)
from .misc_utils import PRINT


# TODO (C4-92, C4-102): Probably to centralize this information in env_utils. Also figure out relation to CGAP.
HIGLASS_BUCKETS = ['elasticbeanstalk-fourfront-webprod-wfoutput',
                   'elasticbeanstalk-fourfront-webdev-wfoutput']


# TODO (C4-92): Centralize this information, it is repeated in other repos
# TODO (C4-102): Does this need to include CGAP envs? As part of the same list, or as a separate list?
PRODUCTION_ENVS = ['fourfront-blue', 'fourfront-green']

# S3 URL identifier
S3_BUCKET_DOMAIN_SUFFIX = '.s3.amazonaws.com'


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
            # PyCharm notes this is unused. -kmp 17-Jul-2020
            # res_json = {}
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


def purge_request_with_retries(request_fxn, url, auth, verb, **kwargs):
    """
    Example of using a non-standard retry function. This one is for purges,
    which return a 423 if the item is locked. This function returns a list of
    locked items to faciliate easier purging
    """
    final_res = None
    error = None
    retry = 0
    # 423 is not included here because it is handled specially
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
            else:
                # handle locked items
                if res.status_code == 423:
                    locked_items = err_reason.get('comment', [])
                    if locked_items:
                        final_res = res
                        error = None
                        break
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


REQUESTS_VERBS = {
    'GET': requests.get,
    'POST': requests.post,
    'PATCH': requests.patch,
    'PUT': requests.put,
    'DELETE': requests.delete,
}


def authorized_request(url, auth=None, ff_env=None, verb='GET',
                       retry_fxn=standard_request_with_retries, **kwargs):
    """
    Generalized function that handles authentication for any type of request to FF.
    Takes a required url, request verb, auth, fourfront environment, and optional
    retry function and headers. Any other kwargs provided are also past into the request.
    For example, provide a body to a request using the 'data' kwarg.
    Timeout of 60 seconds used by default but can be overwritten as a kwarg.

    Verb should be one of: GET, POST, PATCH, PUT, or DELETE
    auth should be obtained using s3Utils.get_key.
    If not provided, try to get the key using s3_utils if 'ff_env' in kwargs

    usage:
    authorized_request('https://data.4dnucleome.org/<some path>', (authId, authSecret))
    OR
    authorized_request('https://data.4dnucleome.org/<some path>', ff_env='fourfront-webprod')
    """
    # Save to uncomment if debugging unit tests...
    # print("authorized_request\n URL=%s\n auth=%s\n ff_env=%s\n verb=%s\n" % (url, auth, ff_env, verb))
    use_auth = unified_authentication(auth, ff_env)
    headers = kwargs.get('headers')
    if not headers:
        kwargs['headers'] = {'content-type': 'application/json', 'accept': 'application/json'}
    if 'timeout' not in kwargs:
        kwargs['timeout'] = 60  # default timeout

    try:
        the_verb = REQUESTS_VERBS[verb.upper()]
    except KeyError:
        raise ValueError("Provided verb %s is not valid. Must be one of: %s"
                         % (verb.upper(), ', '.join(REQUESTS_VERBS.keys())))
    # automatically detect a search and overwrite the retry if it is standard
    if '/search/' in url and retry_fxn == standard_request_with_retries:
        retry_fxn = search_request_with_retries
    # use the given retry function. MUST TAKE THESE PARAMS!
    result = retry_fxn(the_verb, url, use_auth, verb, **kwargs)
    # Save to uncomment if debugging unit tests...
    # print("authorized_request result=", json.dumps(result.json(), indent=2))
    return result


def _sls(val):
    """general helper to check for and strip leading slashes on ids in API fxns
    """
    return val.lstrip('/')


def get_metadata(obj_id, key=None, ff_env=None, check_queue=False, add_on=''):
    """
    Function to get metadata for a given obj_id (uuid or @id, most likely).
    Either takes a dictionary form authentication (MUST include 'server')
    or a string fourfront-environment.
    Also a boolean 'check_queue', which if True
    will use information from the queues and/or datastore=database to
    ensure that the metadata is accurate.
    Takes an optional string add_on that should contain things like
    "frame=object". Join query parameters in the add_on using "&", e.g.
    "frame=object&force_md5"
    *REQUIRES ff_env if check_queue is used*
    """
    auth = get_authentication_with_server(key, ff_env)
    if check_queue and stuff_in_queues(ff_env, check_secondary=False):
        add_on += '&datastore=database'
    get_url = '/'.join([auth['server'], _sls(obj_id)]) + process_add_on(add_on)
    # check the queues if check_queue is True
    response = authorized_request(get_url, auth=auth, verb='GET')
    return get_response_json(response)


def patch_metadata(patch_item, obj_id='', key=None, ff_env=None, add_on=''):
    """
    Patch metadata given the patch body and an optional obj_id (if not provided,
    will attempt to use accession or uuid from patch_item body).
    Either takes a dictionary form authentication (MUST include 'server')
    or a string fourfront-environment.
    """
    auth = get_authentication_with_server(key, ff_env)
    obj_id = obj_id if obj_id else patch_item.get('accession', patch_item.get('uuid'))
    if not obj_id:
        raise Exception("ERROR getting id from given object %s for the request to"
                        " patch item. Supply a uuid or accession." % obj_id)
    patch_url = '/'.join([auth['server'], _sls(obj_id)]) + process_add_on(add_on)
    # format item to json
    patch_item = json.dumps(patch_item)
    response = authorized_request(patch_url, auth=auth, verb='PATCH', data=patch_item)
    return get_response_json(response)


def post_metadata(post_item, schema_name, key=None, ff_env=None, add_on=''):
    """
    Post metadata given the post body and a string schema name.
    Either takes a dictionary form authentication (MUST include 'server')
    or a string fourfront-environment.
    add_on is the string that will be appended to the post url (used
    with tibanna)
    """
    auth = get_authentication_with_server(key, ff_env)
    post_url = '/'.join([auth['server'], schema_name]) + process_add_on(add_on)
    # format item to json
    post_item = json.dumps(post_item)
    response = authorized_request(post_url, auth=auth, verb='POST', data=post_item)
    return get_response_json(response)


def upsert_metadata(upsert_item, schema_name, key=None, ff_env=None, add_on=''):
    """
    UPSERT metadata given the upsert body and a string schema name.
    UPSERT means POST or PATCH on conflict.
    Either takes a dictionary form authentication (MUST include 'server')
    or a string fourfront-environment.
    This function checks to see if an existing object already exists
    with the same body, and if so, runs a patch instead.
    add_on is the string that will be appended to the upsert url (used
    with tibanna)
    """
    auth = get_authentication_with_server(key, ff_env)
    upsert_url = '/'.join([auth['server'], schema_name]) + process_add_on(add_on)
    # format item to json
    upsert_item = json.dumps(upsert_item)
    try:
        response = authorized_request(upsert_url, auth=auth, verb='POST', data=upsert_item)
    except Exception as e:
        # this means there was a conflict. try to patch
        if '409' in str(e):
            return patch_metadata(json.loads(upsert_item), key=auth, add_on=add_on)
        else:
            raise Exception(str(e))
    return get_response_json(response)


def get_search_generator(search_url, auth=None, ff_env=None, page_limit=50):
    """
    Returns a generator given a search_url (which must contain server!), an
    auth and/or ff_env, and an int page_limit, which is used to determine how
    many results are returned per page (i.e. per iteration of the generator)

    Paginates by changing the 'from' query parameter, incrementing it by the
    page_limit size until fewer results than the page_limit are returned.
    If 'limit' is specified in the query, the generator will stop when that many
    results are collectively returned.
    """
    url_params = get_url_params(search_url)
    # indexing below is needed because url params are returned in lists
    curr_from = int(url_params.get('from', ['0'])[0])  # use query 'from' or 0 if not provided
    initial_from = curr_from
    search_limit = url_params.get('limit', ['all'])[0]  # use limit=all by default

    if search_limit != 'all':
        search_limit = int(search_limit)
    url_params['limit'] = [str(page_limit)]
    if not url_params.get('sort'):  # sort needed for pagination
        url_params['sort'] = ['-date_created']
    # stop when fewer results than the limit are returned
    last_total = None
    while last_total is None or last_total == page_limit:
        if search_limit != 'all' and curr_from - initial_from >= search_limit:
            break
        url_params['from'] = [str(curr_from)]  # use from to drive search pagination
        search_url = update_url_params_and_unparse(search_url, url_params)
        # use a different retry_fxn, since empty searches are returned as 400's
        response = authorized_request(search_url, auth=auth, ff_env=ff_env,
                                      retry_fxn=search_request_with_retries)
        try:
            search_res = get_response_json(response)['@graph']
        except KeyError:
            raise('Cannot get "@graph" from the search request for %s. Response '
                  'status code is %s.' % (search_url, response.status_code))
        last_total = len(search_res)
        curr_from += last_total
        if search_limit != 'all' and curr_from - initial_from > search_limit:
            limit_diff = curr_from - initial_from - search_limit
            yield search_res[:-limit_diff]
        else:
            yield search_res


def search_result_generator(page_generator):
    """
    Simple wrapper function to return a generator to iterate through item
    results on the search page

    NOTE: Depending on the nature of the page generator, which may involve separate external calls
    to a resource like elastic search that is not transactionally managed, the data being queried
    may change between those calls, usually to add (though theoretically even to remove) an element.

    Consider a case where the data to be queried is indexed in elastic search as A,C,E,G,I,K,M.
    but where a page size of 3 is used with start position 0. That call will return A,C,E. The
    user may expect G,I on the second page, but before it can be done, suppose an element D is
    indexed and that the stored data is A,C,D,E,G,I,K,M. Requesting data from start position 0 would
    now return A,C,D but we already had the first page, so we request data starting at position 3
    for the second page and get E,G,I.  That means our sequence of return values would be A,C,E,E,G,I,K,M,
    or, in other words, showing a duplication. To avoid this, we keep track of the IDs we've seen
    and show only the first case of each element, so A,C,E,G,I,K,M. (We won't see the D but we weren't
    going to see it anyway, and it wasn't available the time we started, so the timing was already close.)

    Unfortunately, we aren't so lucky for deletion, though that happens more rarely. That will cause
    an element to fall out. So if we have A,C,E,G,I,K,M and C is deleted between the first and second call,
    getting us A,C,E first, and then on the second call when the data is A,E,G,I,K,M we get I,K,M, we'll
    get the sequence A,C,E,I,K,M and will have missed legitimate element G. There is little to do with this
    without restarting (which might not terminate or might be O(n^2) in worst case). But deletion is unusual.
    """
    items_seen = set()
    for page in page_generator:
        for item in page:
            if isinstance(item, dict):
                item_uuid = item.get('uuid')
                if item_uuid:
                    if item_uuid in items_seen:
                        continue
                    items_seen.add(item_uuid)
            yield item


def search_metadata(search, key=None, ff_env=None, page_limit=50, is_generator=False):
    """
    Make a get request of form <server>/<search> and returns a list of results
    using a paginated generator. Include all query params in the search string.
    If is_generator is True, return a generator that yields individual search
    results. Otherwise, return all results in a list (default)
    Either takes a dictionary form authentication (MUST include 'server')
    or a string fourfront-environment.
    """
    auth = get_authentication_with_server(key, ff_env)
    if search.startswith('/'):
        search = search[1:]
    parsed_search = urlparse(search)
    if parsed_search.scheme == '' and parsed_search.netloc == '':  # both will be empty for non-urls
        search_url = '/'.join([auth['server'], search])
    else:
        search_url = search  # assume full url is correct
    page_generator = get_search_generator(search_url, auth=auth, page_limit=page_limit)
    if is_generator:
        # yields individual items from search result
        return search_result_generator(page_generator)
    else:
        # return a list of all search results
        search_res = []
        items_seen = set()
        for page in page_generator:
            for item in page:
                if isinstance(item, dict):
                    item_uuid = item.get('uuid')
                    if item_uuid:
                        if item_uuid in items_seen:
                            continue
                        items_seen.add(item_uuid)
                search_res.append(item)
        return search_res


def get_item_facets(item_type, key=None, ff_env=None):
    """
    Gets facet query string information ie: mapping from facet to query string
    """
    resp = get_metadata('profiles/' + item_type + '.json', key=key, ff_env=ff_env)
    facets = {}
    for query_url, info in resp.get('facets', {}).items():
        facets[info['title']] = query_url

    # status/limit is hardcoded in search.py, so the same must be done here
    facets['Status'] = 'status'
    facets['Limit'] = 'limit'
    return facets


def get_item_facet_values(item_type, key=None, ff_env=None):
    """
    Gets all facets and returns all possible values for each one with counts
    ie: dictionary of facets mapping to a dictionary containing all possible values
    for that facet mapping to the count for that value
    format: {'Project': {'4DN': 2, 'Other': 6}, 'Lab': {...}}
    """
    resp = get_metadata('search/?type=' + item_type, key=key, ff_env=ff_env)['facets']
    facets = {}
    for facet in resp:
        name = facet['title']
        facets[name] = {}
        for term in facet['terms']:
            facets[name][term['key']] = term['doc_count']
    return facets


def faceted_search(key=None, ff_env=None, item_type=None, **kwargs):
    """
    Wrapper method for `search_metadata` that provides an easier way to search
    items based on facets

    kwargs should contain the following 5 things:
        - key (if not using built in aws auth)
        - ff_env (if not using build in aws auth)
        - item_type (if not searching for experiment sets)
        - item_facets (if you don't want to resolve these in this function)
        + any facets (| seperated values) you'd like to search on (see example below)

    Example: search for all experiments under the 4DN project with experiment type
    Dilution Hi-C
        kwargs = { 'Project': '4DN',
                   'Experiment Type': 'Dilution Hi-C',
                   'key': key,
                   'ff_env': ff_env,
                   'item_type': 'ExperimentSetReplicate' }
        results = faceted_search(**kwargs)
    """
    item_facets = kwargs.get('item_facets', None)
    item_type = 'ExperimentSetReplicate' if item_type is None else item_type
    search = 'search/?type=' + item_type
    if item_facets is None:
        item_facets = get_item_facets(item_type, key=key, ff_env=ff_env)
    for facet, values in kwargs.items():
        if facet != 'item_type':
            if facet in item_facets:
                for value in values.split('|'):
                    fmt_value = '+'.join(value.split())
                    if fmt_value[0] == '-':  # handle negative
                        search = search + '&' + item_facets[facet] + '!=' + fmt_value[1:]
                    else:
                        search = search + '&' + item_facets[facet] + '=' + fmt_value
    return search_metadata(search, ff_env=ff_env, key=key)


def fetch_files_qc_metrics(data, associated_files=None,
                           ignore_typical_fields=True,
                           key=None, ff_env=None):
    """
    Utility function to grab all the qc metrics from associated types of file such as:
    'proccessed_files', 'other_processed_files', 'files'
    Args:
        data: the metadata of a ExperimentSet or Experiment
        associated_files: a list of the types of the files fields the qc metrics will be extracted from:
            examples are = ['files', 'processed_files', 'other_processed_files']
        ignore_typical_fields: flag to ignore 4DN custom fields from the qc metric object
        key: authentication key for ff_env (see get_authentication_with_server)
        ff_env: The relevant ff beanstalk environment name.

    Returns:
        a dictionary of dictionaries containing the qc_metric information
    """
    if associated_files is None:
        associated_files = ['processed_files']

    qc_metrics = {}

    if ignore_typical_fields:
        ignorable_qc_fields = ['contributing_labs', 'schema_version', 'external_references', '@context', 'aliases',
                               'project_release', 'award', 'principals_allowed', 'validation-errors',
                               'last_modified', 'slope', '@id', 'aggregated-items', 'status', 'public_release',
                               'actions', 'submitted_by', 'convergence', 'lab', 'date_created', 'uuid']
    else:
        ignorable_qc_fields = []
    # for each file
    for associated_file in associated_files:
        if associated_file in data:
            if associated_file == 'other_processed_files':
                target_files = []
                for entry in data[associated_file]:
                    if 'files' in entry:
                        target_files = target_files + entry['files']

            else:
                target_files = data[associated_file]

            for entry in target_files:
                if entry.get('quality_metric'):
                    # check if it is a list of qc metrics
                    if entry['quality_metric']['display_title'].startswith('QualityMetricQclist'):
                        qc_metric_list_uuid = entry['quality_metric']['uuid']
                        qc_metric_list = get_metadata(qc_metric_list_uuid, key=key, ff_env=ff_env)
                        if not qc_metric_list.get('qc_list'):
                            continue
                        for qc in qc_metric_list['qc_list']:
                            qc_uuid = qc['value']['uuid']
                            qc_meta = get_metadata(qc_uuid, key=key, ff_env=ff_env)
                            qc_values = {k: v for k, v in qc_meta.items() if k not in ignorable_qc_fields}
                            source_file_association = associated_file if associated_file != 'files' else 'raw_file'
                            source_file = entry['accession']
                            source_file_type = entry['file_type_detailed']
                            qc_info = {
                                qc_uuid: {'values': qc_values,
                                          'source_file_association': source_file_association,
                                          'source_file': source_file,
                                          'source_file_type': source_file_type
                                          }
                            }
                            qc_metrics.update(qc_info)

                    else:
                        qc_uuid = entry['quality_metric']['uuid']
                        qc_meta = get_metadata(qc_uuid, key=key, ff_env=ff_env)
                        qc_values = {k: v for k, v in qc_meta.items() if k not in ignorable_qc_fields}
                        source_file_association = associated_file if associated_file != 'files' else 'raw_file'
                        source_file = entry['accession']
                        source_file_type = entry['file_type_detailed']
                        qc_info = {
                            qc_uuid: {'values': qc_values,
                                      'source_file_association': source_file_association,
                                      'source_file': source_file,
                                      'source_file_type': source_file_type
                                      }
                        }
                        qc_metrics.update(qc_info)
    return qc_metrics


def get_associated_qc_metrics(uuid, key=None, ff_env=None, include_processed_files=True,
                              include_raw_files=False,
                              include_supplementary_files=False):
    """
    Given a uuid of an experimentSet return a dictionary of dictionaries with each dictionary
    representing a quality metric.

    Args:
        uuid: uuid of an experimentSet
        key: authentication key for ff_env (see get_authentication_with_server)
        ff_env: The relevant ff beanstalk environment name.
        include_processed_files: if False will exclude QC metrics on processed files
                                Default: True
        include_raw_files: if True will provide QC metrics on raw files as well
                           Default: False
        include_supplementary_files: if True will also give QC's associated with
                                     non-processed files. Default: False
    Returns:
        a dictionary of dictionaries with the following structure:
            {<qc_metric_uuid>}:{
                'values': the values of the qc_metric object>,
                'source_file_association': <the file class (processed_file or raw_files)>,
                'source_file': <the accession of the file that the qc is linked to>,
                'source_file_type': <the description of the file that the qc is linked to>,
                'experiment_description': <the description of the experiment or experimentset>
                'organism': <the organism>
                'experiment_type': <the experiment type (in situ Hi-C, ChIP-seq)>,
                'experiment_subclass': <the experiment subclass (Hi-C)>,
                'source_experiment': <the experiment the qc is linked to (if apply)>,
                'source_experimentSet': <the experimentSet the qc is linked to>,
                'biosource_summary': <the experiment biosource>
                }
            }
    """
    result = {}
    associated_files = []

    # Additional information to include in the results for interpretation
    organism = None
    experiment_type = None
    experiment_subclass = None
    biosource_summary = None

    resp = get_metadata(uuid, key=key, ff_env=ff_env)

    # Checks wheter the input is a experiment or experimentset otherwise throws an error
    if 'ExperimentSet' not in resp['@type']:
        raise TypeError('Expected ExperimentSet')

    # verifies what category of files to include (processed_files, other_processed_files, files)
    if include_processed_files:
        associated_files.append('processed_files')
    if include_supplementary_files:
        associated_files.append('other_processed_files')
    if include_raw_files:
        associated_files.append('files')

    if not associated_files:
        return result

    # If it is an experimentset, get qc_metrics for the experiments in the experiment set
    if resp.get('experiments_in_set'):
        organism = resp['experiments_in_set'][0]['biosample']['biosource'][0]['organism']['name']
        # organism = resp['experiments_in_set'][0]['biosample']['biosource'][0]['individual']['organism']['name']
        experiment_type = resp['experiments_in_set'][0]['experiment_type']['display_title']
        experiment_subclass = resp['experiments_in_set'][0]['experiment_type']['assay_subclass_short']
        biosource_summary = resp['experiments_in_set'][0]['biosample']['biosource_summary']

        for exp in resp['experiments_in_set']:
            exp_description = exp['display_title']
            exp_qc_metrics = fetch_files_qc_metrics(exp, associated_files, key=key, ff_env=ff_env)
            meta_info = {'experiment_description': exp_description,
                         'organism': organism,
                         'experiment_type': experiment_type,
                         'experiment_subclass': experiment_subclass,
                         'source_experiment': exp['accession'],
                         'source_experimentSet': resp['accession'],
                         'biosource_summary': biosource_summary
                         }
            if exp_qc_metrics:
                for exp_qc_metric in exp_qc_metrics.values():
                    exp_qc_metric.update(meta_info)
                result.update(exp_qc_metrics)

    description = resp.get('dataset_label', None)
    es_qc_metrics = fetch_files_qc_metrics(resp, associated_files, key=key, ff_env=ff_env)
    if es_qc_metrics:
        meta_info = {'experiment_description': description,
                     'organism': organism,
                     'experiment_type': experiment_type,
                     'experiment_subclass': experiment_subclass,
                     'source_experiment': None,
                     'source_experimentSet': resp['accession'],
                     'biosource_summary': biosource_summary
                     }
        for qc_metric in es_qc_metrics.values():
            qc_metric.update(meta_info)
        result.update(es_qc_metrics)

    return result


def get_metadata_links(obj_id, key=None, ff_env=None):
    """
    Given standard key/ff_env authentication, return result for @@links view
    """
    auth = get_authentication_with_server(key, ff_env)
    links_url = '/'.join([auth['server'], obj_id, '@@links'])
    response = authorized_request(links_url, auth=auth, verb='GET')
    return get_response_json(response)


def delete_metadata(obj_id, key=None, ff_env=None):
    """
    Given standard key/ff_env authentication, simply set the status of the
    given object to 'deleted'
    """
    return patch_metadata({'status': 'deleted'}, obj_id, key, ff_env)


def purge_metadata(obj_id, key=None, ff_env=None):
    """
    Given standard key/ff_env authentication, attempt to purge the item from
    the DB (FULL delete). If the item cannot be deleted due to other items
    still linking it, this function provides information in the response
    `@graph`
    """
    auth = get_authentication_with_server(key, ff_env)
    purge_url = '/'.join([auth['server'], obj_id]) + '?purge=True'
    response = authorized_request(purge_url, auth=auth, verb='DELETE',
                                  retry_fxn=purge_request_with_retries)
    return get_response_json(response)


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
    patch_url = '/'.join([auth['server'], obj_id]) + delete_str
    # use an empty patch body
    response = authorized_request(patch_url, auth=auth, verb='PATCH', data=json.dumps({}))
    return get_response_json(response)


def get_es_search_generator(es_client, index, body, page_size=200):
    """
    Simple generator behind get_es_metadata which takes an es_client (from
    es_utils create_es_client), a string index, and a dict query body.
    Also takes an optional string page_size, which controls pagination size
    NOTE: 'index' must be namespaced
    """
    search_total = None
    covered = 0
    while search_total is None or covered < search_total:
        es_res = es_client.search(index=index, body=body, size=page_size, from_=covered)
        if search_total is None:
            search_total = es_res['hits']['total']
        es_hits = es_res['hits']['hits']
        covered += len(es_hits)
        yield es_hits


def get_es_metadata(uuids, es_client=None, filters=None, sources=None, chunk_size=200,
                    is_generator=False, key=None, ff_env=None):
    """
    Given a list of string item uuids, will return a
    dictionary response of the full ES record for those items (or an empty
    dictionary if the items don't exist/ are not indexed)
    Returns
        A dictionary with following keys
            -keys with metadata
                properties (raw frame without uuid), embedded, object
            -keys summarizing interactions
                linked_uuids_object, linked_uuids_embedded, links, rev_linked_to_me
            -others
                paths, aggregated_items, rev_link_names, item_type, principals_allowed,
                unique_keys, sid, audit, uuid, propsheets
    Args
        uuids:
            list of uuids to fetch from ES
        es_client:
            You can pass in an Elasticsearch client
            (initialized by create_es_client)
            through the es_client param to save init time.
        filters:
            Advanced users can optionally pass a dict of filters that will be added
            to the Elasticsearch query.
                For example: filters={'status': 'released'}
                You can also specify NOT fields:
                    example: filters={'status': '!released'}
                You can also specifiy lists of values for fields:
                    example: filters={'status': ['released', archived']}
            NOTES:
                - different filter field are combined using AND queries (must all match)
                    example: filters={'status': ['released'], 'public_release': ['2018-01-01']}
                - values for the same field and combined with OR (such as multiple statuses)
        sources:
            You may also specify which fields are returned from ES by specifying a
            list of source fields with the sources argument.
            This field MUST include the full path of the field, such as 'embedded.uuid'
            (for the embedded frame) or 'object.uuid' for the object frame. You may
            also use the wildcard, such as 'embedded.*' for all fields in the embedded
            frame.
            You need to follow the dictionary structure of the get_es_metadata result
            i.e. for getting uuids on the linked field 'files'
                sources = ['properties.files']
                or
                sources = ['embedded.files.uuid']
            i.e. getting all fields for lab in embedded frame
                sources = ['embedded.lab.*']
            i.e. for getting a only object frame
                sources = ['object.*']
        chunk_size:
            Integer chunk_size may be used to control the number of uuids that are
            passed to Elasticsearch in each query; setting this too high may cause
            ES reads to timeout.
        is_generator:
            Boolean is_generator will return a generator for individual results if True;
            if False (default), returns a list of results.
        key: authentication key for ff_env (see get_authentication_with_server)
        ff_env: authentication by env (needs system variables)
    """
    auth = get_authentication_with_server(key, ff_env)
    meta = _get_es_metadata(uuids, es_client, filters or {}, sources or [], chunk_size, auth)
    if is_generator:
        return meta
    return list(meta)


def _get_es_metadata(uuids, es_client, filters, sources, chunk_size, auth):
    """
    Internal function needed because there are multiple levels of iteration
    used to create the generator.
    Should NOT be used directly
    """
    health = get_health_page(key=auth)
    if es_client is None:
        es_url = health['elasticsearch']
        es_client = es_utils.create_es_client(es_url, use_aws_auth=True)
    namespace_star = health.get('namespace', '') + '*'
    # match all given uuids to _id fields
    # sending in too many uuids in the terms query can crash es; break them up
    # into groups of max size 100
    for i in range(0, len(uuids), chunk_size):
        query_uuids = uuids[i:i + chunk_size]
        es_query = {
            'query': {
                'bool': {
                    'must': [
                        {'terms': {'_id': query_uuids}}
                    ],
                    'must_not': []
                }
            },
            'sort': [{'_uid': {'order': 'desc'}}]
        }
        if filters:
            if not isinstance(filters, dict):
                raise Exception('Invalid filters for get_es_metadata: %s' % filters)
            else:
                for k, v in filters.items():
                    key_terms = []
                    key_not_terms = []
                    iter_terms = [v] if not isinstance(v, list) else v
                    for val in iter_terms:
                        if val.startswith('!'):
                            key_not_terms.append(val[1:])
                        else:
                            key_terms.append(val)
                    if key_terms:
                        es_query['query']['bool']['must'].append(
                            {'terms': {'embedded.' + k + '.raw': key_terms}}
                        )
                    if key_not_terms:
                        es_query['query']['bool']['must_not'].append(
                            {'terms': {'embedded.' + k + '.raw': key_not_terms}}
                        )
        if sources:
            if not isinstance(sources, list):
                raise Exception('Invalid sources for get_es_metadata: %s' % sources)
            else:
                es_query['_source'] = sources
        # use chunk_limit as page size for performance reasons
        for es_page in get_es_search_generator(es_client, namespace_star, es_query,
                                               page_size=chunk_size):
            for hit in es_page:
                yield hit['_source']  # yield individual items from ES


def get_schema_names(key=None, ff_env=None):
    """
    Create a dictionary of all schema names to item class names
    i.e. FileFastq: file_fastq

    Args:
        key (dict):                      standard ff_utils authentication key
        ff_env (str):                    standard ff environment string

    Returns:
        dict: contains key schema names and value item class names
    """
    auth = get_authentication_with_server(key, ff_env)
    schema_name = {}
    profiles = get_metadata('profiles/', key=auth, add_on='frame=raw')
    for key, value in profiles.items():
        # skip abstract types
        if value.get('isAbstract') is True:
            continue
        # some test schemas in local don't have the id field
        schema_filename = value.get('id')
        if schema_filename:
            schema_name[key] = schema_filename.split('/')[-1][:-5]
    return schema_name


def expand_es_metadata(uuid_list, key=None, ff_env=None, store_frame='raw', add_pc_wfr=False, ignore_field=None,
                       use_generator=False, es_client=None):
    """
    starting from list of uuids, tracks all linked items in object frame by default
    if you want to add processed files and workflowruns, you can change add_pc_wfr to True
    returns a dictionary with item types (schema name), and list of items in defined frame
    Sometimes, certain fields need to be skipped (i.e. relations), you can use ignore fields.
    Args:
        uuid_list (list):                Starting node for search, only use uuids.
        key (dict):                      standard ff_utils authentication key
        ff_env (str):                    standard ff environment string
        store_frame (str, default 'raw'):Depending on use case, can store frame raw or object or embedded
                                         Note: If you store in embedded, the total collection can have references
                                         to the items that are not in the store
        add_pc_wfr (bool):               Include workflow_runs and linked items (processed/ref files, wf, software...)
        ignore_field(list):              Remove keys from items, so any linking through these fields, ie relations
        use_generator (bool):            Use a generator when getting es. Less memory used but takes longer
        es_client:                       optional result from es_utils.create_es_client - note this could be regenerated
                                         in this method if the signature expires
    Returns:
        dict: contains all item types as keys, and with values of list of dictionaries
              i.e.
              {
                  'experiment_hi_c': [ {'uuid': '1234', '@id': '/a/b/', ...}, {...}],
                  'experiment_set': [ {'uuid': '12345', '@id': '/c/d/', ...}, {...}],
              }
        list: contains all uuids from all items.

    # TODO: if more file types (currently FileFastq and FileProcessed) get workflowrun calculated properties
            we need to add them to the add_from_embedded dictionary.
    """
    if ignore_field is None:
        ignore_field = []
    # assert that the used parameter is correct
    accepted_frames = ['raw', 'object', 'embedded']
    if store_frame not in accepted_frames:
        raise ValueError('Invalid frame name "{}", please use one of {}'.format(store_frame, accepted_frames))

    # wrap key remover, used multiple times
    def remove_keys(my_dict, remove_list):
        if remove_list:
            for del_field in remove_list:
                if del_field in my_dict:
                    del my_dict[del_field]
        return my_dict

    auth = get_authentication_with_server(key, ff_env)
    es_url = None
    if es_client is None:  # set up an es client if none is provided
        es_url = get_health_page(key=auth)['elasticsearch']
        es_client = es_utils.create_es_client(es_url, use_aws_auth=True)

    # creates a dictionary of schema names to collection names
    schema_name = get_schema_names(key=auth)

    # keep list of fields that only exist in frame embedded (revlinks, calcprops) that you want connected
    if add_pc_wfr:
        add_from_embedded = {'file_fastq': ['workflow_run_inputs', 'workflow_run_outputs'],
                             'file_processed': ['workflow_run_inputs', 'workflow_run_outputs']
                             }
    else:
        add_from_embedded = {}
    store = {}
    item_uuids = set()  # uuids we've already stored
    chunk = 100  # chunk the requests - don't want to hurt es performance

    while uuid_list:
        uuids_to_check = []  # uuids to add to uuid_list if not if not in item_uuids

        # get the next page of data, recreating the es_client if need be
        try:
            current_page = get_es_metadata(uuid_list, es_client=es_client, chunk_size=chunk,
                                           is_generator=use_generator, key=auth)
        except AuthorizationException:  # our signature expired, recreate the es_client with a fresh signature
            if es_url:
                es_client = es_utils.create_es_client(es_url, use_aws_auth=True)
            else:  # recreate client and try again - if we fail here, exception should propagate
                es_url = get_health_page(key=auth)['elasticsearch']
                es_client = es_utils.create_es_client(es_url, use_aws_auth=True)

            current_page = get_es_metadata(uuid_list, es_client=es_client, chunk_size=chunk,
                                           is_generator=use_generator, key=auth)
        for es_item in current_page:
            # get object type via es result and schema for storing
            obj_type = es_item['object']['@type'][0]
            obj_key = schema_name[obj_type]
            if obj_key not in store:
                store[obj_key] = []
            # add raw frame to store and uuid to list
            uuid = es_item['uuid']
            if uuid not in item_uuids:
                # get the desired frame from the ES response
                if store_frame == 'object':
                    frame_resp = remove_keys(es_item['object'], ignore_field)
                elif store_frame == 'embedded':
                    frame_resp = remove_keys(es_item['embedded'], ignore_field)
                else:
                    frame_resp = remove_keys(es_item['properties'], ignore_field)
                    frame_resp['uuid'] = uuid  # uuid is not in properties, so add it
                store[obj_key].append(frame_resp)
                item_uuids.add(uuid)
            else:  # this case should not happen
                raise Exception('Item %s aded twice in expand_es_metadata, should not happen' % uuid)

            # get linked items from es
            for key in es_item['links']:
                skip = False
                # if link is from ignored_field, skip
                if key in ignore_field:
                    skip = True
                # sub embedded objects have a different naming str:
                for ignored in ignore_field:
                    if key.startswith(ignored + '~'):
                        skip = True
                if skip:
                    continue
                uuids_to_check.extend(es_item['links'][key])

            # check if any field from the embedded frame is required
            add_fields = add_from_embedded.get(obj_key)
            if add_fields:
                for a_field in add_fields:
                    field_val = es_item['embedded'].get(a_field)
                    if field_val:
                        # turn it into string
                        field_val = str(field_val)
                        # check if any of embedded uuids is in the field value
                        es_links = [i['uuid'] for i in es_item['linked_uuids_embedded']]
                        for a_uuid in es_links:
                            if a_uuid in field_val:
                                uuids_to_check.append(a_uuid)

        # get uniques for uuids_to_check and then update uuid_list
        uuids_to_check = set(uuids_to_check)
        uuid_list = list(uuids_to_check - item_uuids)

    return store, list(item_uuids)


def _get_page(*, page, key=None, ff_env=None):
    """ Wrapper for commonly used code to GET a page from an environment
        Given keys or ff_env, will return json containing an error rather than raising an
        exception if this fails, since this function should tolerate failure """
    try:
        auth = get_authentication_with_server(key, ff_env)
        page_res = authorized_request(auth['server'] + page, auth=auth, verb='GET')
        ret = get_response_json(page_res)
    except Exception as exc:
        ret = {'error': str(exc)}
    return ret


def get_health_page(key=None, ff_env=None):
    """
    Simple function to return the json for a FF health page
    """
    return _get_page(page='/health', key=key, ff_env=ff_env)


def get_counts_page(key=None, ff_env=None):
    """ Gets DB/ES counts page in JSON """
    return _get_page(page='/counts', key=key, ff_env=ff_env)


def get_indexing_status(key=None, ff_env=None):
    """ Gets indexing status counts page in JSON """
    return _get_page(page='/indexing_status', key=key, ff_env=ff_env)


# namedtuple definition used below and can be imported elsewhere
CountSummary = namedtuple('CountSummary', ['are_even', 'summary_total'])


def get_counts_summary(env):
    """ Returns a named tuple given an FF name to check representing the counts state.
            CountSummary
                are_even: boolean on whether or not counts are even
                summary_total: raw value of counts
    """
    totals = get_counts_page(ff_env=env)
    if 'error' in totals:  # error encountered getting page, assume false and return error
        return CountSummary(are_even=False, summary_total=totals)
    totals = totals['db_es_total'].split()

    # example value of split totals: ['DB:', '74048', 'ES:', '74048']
    # or ['DB:', '887', 'ES:', '888', '<', 'ES', 'has', '1', 'more', 'items', '>']
    db_total = int(totals[1])
    es_total = int(totals[3])
    if db_total > es_total or es_total > db_total:
        return CountSummary(are_even=False, summary_total=totals)
    return CountSummary(are_even=True, summary_total=totals)


class SearchESMetadataHandler(object):
    """
    Wrapper class for executing lucene queries directly on ES.
    Resolves ES instance location via health page of the given
    environment. Requires AWS permissions to use.
    Can be used directly but is used through search_es_metadata.

    NOTE: use this class directly if you plan on making multiple subsequent requests
    to the same environment.
    """

    def __init__(self, key=None, ff_env=None):
        self.health = get_health_page(key, ff_env)  # expensive - do not call repeatedly!
        self.es_url = self.health['elasticsearch']
        self.client = es_utils.create_es_client(self.es_url)

    def execute_search(self, index, query, is_generator=False, page_size=200):
        """
        Executes lucene query on this client's index.

        :arg index: index to search under
        :arg query: query to run
        :arg is_generator: boolean on whether or not to use a generator
        :arg page_size: if using a generator, how many results to give per request

        :returns: list of results of query or None
        """
        if not is_generator:
            return es_utils.execute_lucene_query_on_es(self.client, index=index, query=query)
        return search_result_generator(get_es_search_generator(self.client, index, query, page_size=page_size))


def search_es_metadata(index, query, key=None, ff_env=None, is_generator=False):
    """
        Executes a lucene search query on on the ES Instance for this
        environment.

        NOTE: It is okay to use this function directly but for repeat usage please use
        SearchESMetadataHandler as it caches an expensive API request to AWS

        :arg index: index to search under
        :arg query: dictionary of query
        :arg key: optional, 2-tuple authentication key (access_key_id, secret)
        :arg ff_env: ff_env to use
        :arg is_generator: boolean on whether or not to use a generator

        :returns: list of results of query or None
    """
    search_handler = SearchESMetadataHandler(key, ff_env)
    return search_handler.execute_search(index, query, is_generator)


#####################
# Utility functions #
#####################
def unified_authentication(auth=None, ff_env=None):
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
        # TODO: The ff_env argument is mis-named, something we should fix sometime. It can be a cgap env, too.
        auth = s3_utils.s3Utils(env=ff_env).get_access_keys()
    # see if auth is directly from get_access_keys()
    use_auth = None
    # needed for old form of auth from get_key()
    if isinstance(auth, dict) and isinstance(auth.get('default'), dict):
        auth = auth['default']
    if isinstance(auth, dict) and 'key' in auth and 'secret' in auth:
        use_auth = (auth['key'], auth['secret'])
    elif isinstance(auth, tuple) and len(auth) == 2:
        use_auth = auth
    if not use_auth:
        raise ValueError("Must provide a valid authorization key or ff environment."
                         " You gave: %s (key), %s (ff_env)" % (auth, ff_env))
    return use_auth


def get_authentication_with_server(auth=None, ff_env=None):
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
            # TODO: Refactor these ValueErrors to do messaging in an error class where it doesn't clutter functionality.
            raise ValueError("ERROR GETTING SERVER!"
                             "\nMust provide dictionary auth with 'server' or ff environment."
                             " You gave: %s (auth), %s (ff_env)"
                             % (auth, ff_env))
        auth = s3_utils.s3Utils(env=ff_env).get_access_keys()
        if 'server' not in auth:
            raise ValueError("ERROR GETTING SERVER!"
                             "\nAuthentication retrieved using ff environment does not have server information."
                             " Found: %s (auth), %s (ff_env)" % (auth, ff_env))
    # ensure that the server does not end with '/'
    if auth['server'].endswith('/'):
        auth['server'] = auth['server'][:-1]
    return auth


def stuff_in_queues(ff_env_index_namespace, check_secondary=False):
    """
    Used to guarantee up-to-date metadata by checking the contents of the indexer queues.
    If items are currently waiting in the primary queue, return False.
    If check_secondary is True, will also check the secondary queue.
    """
    if not ff_env_index_namespace:
        raise ValueError(f"Must provide a full fourfront environment name to this function"
                         f" (such as 'fourfront-webdev'). You gave: {ff_env_index_namespace!r}")
    stuff_in_queue = False
    client = boto3.client('sqs', region_name='us-east-1')
    queue_names = ['-indexer-queue']
    if check_secondary:
        queue_names.append('-secondary-indexer-queue')
    for queue_name in queue_names:
        try:
            queue_url = client.get_queue_url(
                QueueName=ff_env_index_namespace + queue_name
            ).get('QueueUrl')
            queue_attrs = client.get_queue_attributes(
                QueueUrl=queue_url,
                AttributeNames=['ApproximateNumberOfMessages', 'ApproximateNumberOfMessagesNotVisible']
            ).get('Attributes', {})
        except Exception:
            PRINT('Error finding queue or its attributes: %s' % ff_env_index_namespace + queue_name)
            stuff_in_queue = True  # queue not found. use datastore=database
            break
        else:
            visible = queue_attrs.get('ApproximateNumberOfMessages', '-1')
            not_vis = queue_attrs.get('ApproximateNumberOfMessagesNotVisible', '-1')
            if (visible and int(visible) > 0) or (not_vis and int(not_vis) > 0):
                stuff_in_queue = True
                break
    return stuff_in_queue


def fetch_network_ids(subnet_names, security_group_names):
    """ Takes lists of subnet and security group names, and fetches their corresponding ids from AWS."""
    ec2_client = boto3.client('ec2')
    subnet_ids = []
    security_group_ids = []
    for i in subnet_names:
        response = ec2_client.describe_subnets()
        subnet_id = i
        subnet_ids.append(subnet_id)
    for i in security_group_names:
        response = ec2_client.describe_security_groups(  # could possibly be done with just one query
            Filters=[
                dict(Name='group-name', Values=[i])
            ]
        )
        security_group_id = response['SecurityGroups'][0]['GroupId']
        security_group_ids.append(security_group_id)
    return subnet_ids, security_group_ids


def get_response_json(res):
    """
    Very simple function to return json from a response or raise an error if
    it is not present. Used with the metadata functions.
    """
    try:
        res_json = res.json()
    except Exception:
        raise Exception('Cannot get json for request to %s. Status'
                        ' code: %s. Response text: %s' %
                        (res.url, res.status_code, res.text))
    return res_json


def process_add_on(add_on):
    """
    simple function to ensure that a query add on string starts with "?"
    """
    if add_on.startswith('&'):
        add_on = '?' + add_on[1:]
    if add_on and not add_on.startswith('?'):
        add_on = '?' + add_on
    return add_on


def get_url_params(url):
    """
    Returns a dictionary of url params using parse_qs.
    Example: get_url_params('<server>/search/?type=Biosample&limit=5') returns
    {'type': ['Biosample'], 'limit': '5'}
    """
    parsed_url = urlparse(url)
    return parse_qs(parsed_url.query)


def update_url_params_and_unparse(url, url_params):
    """
    Takes a string url and url params (in format of what is returned by
    get_url_params). Returns a string url param with newly formatted params
    """
    # Note: Although it upsets linting tools, ._replace() is an advertised interface of url.parse -kmp 17-Oct-2020
    #       See https://docs.python.org/3/library/urllib.parse.html
    parsed_url = urlparse(url)._replace(query=urlencode(url_params, True))  # noQA
    return urlunparse(parsed_url)


def convert_param(parameter_dict, vals_as_string=False):
    """
    converts dictionary format {argument_name: value, argument_name: value, ...}
    to {'workflow_argument_name': argument_name, 'value': value}
    """
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

    # Not needed? -kmp & Will 30-Mar-2020
    # print(str(metadata_parameters))
    return metadata_parameters


def generate_rand_accession(project_prefix='4DN', item_prefix='FI'):
    rand_accession = ''
    for i in range(7):
        r = random.choice('ABCDEFGHIJKLMNOPQRSTUVWXYZ123456789')
        rand_accession += r
    return ''.join([project_prefix, item_prefix, rand_accession])


def dump_results_to_json(store, folder):
    """Takes resuls from expand_es_metadata, and dumps them into the given folder in json format.
    Args:
        store (dict): results from expand_es_metadata
        folder:       folder for storing output
    """
    if folder[-1] == '/':
        folder = folder[:-1]
    if not os.path.exists(folder):
        os.makedirs(folder)
    for a_type in store:
        filename = folder + '/' + a_type + '.json'
        with open(filename, 'w') as outfile:
            json.dump(store[a_type], outfile, indent=4)


def parse_s3_bucket_and_key_url(url: str) -> (str, str):
    """ Parses the given s3 URL into its pair of bucket, key
        Note that this function works the way it does because of how these
        urls end up in our database. Eventually we should clean this up.
        Format:
            https://s3.amazonaws.com/cgap-devtest-main-application-cgap-devtest-wfout/GAPFI1HVXJ5F/fastqc_report.html
            https://cgap-devtest-main-application-tibanna-logs.s3.amazonaws.com/41c2fJDQcLk3.metrics/metrics.html
    """
    parsed_url = urlparse(url)
    if parsed_url.hostname.endswith(S3_BUCKET_DOMAIN_SUFFIX):
        bucket = parsed_url.hostname[:-len(S3_BUCKET_DOMAIN_SUFFIX)]
        key = parsed_url.path.lstrip('/')
    else:
        [bucket, key] = parsed_url.path.lstrip('/').split('/', 1)
    return bucket, key
