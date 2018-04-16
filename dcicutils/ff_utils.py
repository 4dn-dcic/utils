import datetime
import json
import time
from uuid import uuid4, UUID
import random
import copy
from . import s3_utils
import requests

from dcicutils import submit_utils


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


def create_ffmeta_awsem(workflow, app_name, input_files=None, parameters=None, title=None, uuid=None,
                        output_files=None, award='1U01CA200059-01', lab='4dn-dcic-lab',
                        run_status='started', run_platform='AWSEM', run_url='', **kwargs):

    input_files = [] if input_files is None else input_files
    parameters = [] if parameters is None else parameters

    if title is None:
        title = app_name + " run " + str(datetime.datetime.now())

    return WorkflowRunMetadata(workflow=workflow, app_name=app_name, input_files=input_files,
                               parameters=parameters, uuid=uuid, award=award,
                               lab=lab, run_platform=run_platform, run_url=run_url,
                               title=title, output_files=output_files, run_status=run_status)


def create_ffmeta(sbg, workflow, input_files=None, parameters=None, title=None, sbg_task_id=None,
                  sbg_mounted_volume_ids=None, sbg_import_ids=None, sbg_export_ids=None, uuid=None,
                  award='1U01CA200059-01', lab='4dn-dcic-lab', run_platform='SBG',
                  output_files=None, run_status='started', **kwargs):

    input_files = [] if input_files is None else input_files
    parameters = [] if parameters is None else parameters
    # TODO: this probably is not right
    sbg_export_ids = [] if sbg_export_ids is None else sbg_export_ids

    if title is None:
        title = sbg.app_name + " run " + str(datetime.datetime.now())

    if sbg_task_id is None:
        sbg_task_id = sbg.task_id

    if not sbg_mounted_volume_ids:
        try:
            sbg.volume_list[0]['name']
            sbg_mounted_volume_ids = [x['name'] for x in sbg.volume_list]
        except:  # noqa: E722
            sbg_mounted_volume_ids = [x for x in sbg.volume_list]

    if not sbg_import_ids:
        sbg_import_ids = sbg.import_id_list

    if not output_files:
        output_files = sbg.export_report
    else:
        # self.output_files may contain e.g. file_format and file_type information.
        for of in output_files:
            for of2 in sbg.export_report:
                if of['workflow_argument_name'] == of2['workflow_argument_name']:
                    for k, v in of2.items():
                        of[k] = v

    return WorkflowRunMetadata(workflow, sbg.app_name, input_files, parameters,
                               sbg_task_id, sbg_import_ids, sbg_export_ids,
                               sbg_mounted_volume_ids, uuid,
                               award, lab, run_platform, title, output_files, run_status, **kwargs)


class WorkflowRunMetadata(object):
    '''
    fourfront metadata
    '''

    def __init__(self, workflow, app_name, input_files=[],
                 parameters=[], sbg_task_id=None,
                 sbg_import_ids=None, sbg_export_ids=None,
                 sbg_mounted_volume_ids=None, uuid=None,
                 award='1U01CA200059-01', lab='4dn-dcic-lab',
                 run_platform='SBG', title=None, output_files=None,
                 run_status='started', awsem_job_id=None,
                 run_url='', **kwargs):
        """Class for WorkflowRun that matches the 4DN Metadata schema
        Workflow (uuid of the workflow to run) has to be given.
        Workflow_run uuid is auto-generated when the object is created.
        """
        if run_platform == 'SBG':
            self.sbg_app_name = app_name
            # self.app_name = app_name
            if sbg_task_id is None:
                self.sbg_task_id = ''
            else:
                self.sbg_task_id = sbg_task_id
            if sbg_mounted_volume_ids is None:
                self.sbg_mounted_volume_ids = []
            else:
                self.sbg_mounted_volume_ids = sbg_mounted_volume_ids
            if sbg_import_ids is None:
                self.sbg_import_ids = []
            else:
                self.sbg_import_ids = sbg_import_ids
            if sbg_export_ids is None:
                self.sbg_export_ids = []
            else:
                self.sbg_export_ids = sbg_export_ids
        elif run_platform == 'AWSEM':
            self.awsem_app_name = app_name
            # self.app_name = app_name
            if awsem_job_id is None:
                self.awsem_job_id = ''
            else:
                self.awsem_job_id = awsem_job_id
        else:
            raise Exception("invalid run_platform {} - it must be either SBG or AWSEM".format(run_platform))

        self.run_status = run_status
        self.uuid = uuid if uuid else str(uuid4())
        self.workflow = workflow
        self.run_platform = run_platform
        if run_url:
            self.run_url = run_url

        self.title = title
        self.input_files = input_files
        if output_files:
            self.output_files = output_files
        else:
            self.output_files = []

        self.parameters = parameters
        self.award = award
        self.lab = lab

    def append_outputfile(self, outjson):
        self.output_files.append(outjson)

    def append_volumes(self, sbg_volume):
        self.sbg_mounted_volume_ids.append(sbg_volume.id)

    def as_dict(self):
        return self.__dict__

    def toJSON(self):
        return json.dumps(self, default=lambda o: o.__dict__, sort_keys=True, indent=4)

    def post(self, key, type_name=None):
        if not type_name:
            if self.run_platform == 'SBG':
                type_name = 'workflow_run_sbg'
            elif self.run_platform == 'AWSEM':
                type_name = 'workflow_run_awsem'
            else:
                raise Exception("cannot determine workflow schema type: SBG or AWSEM?")
        return post_to_metadata(self.as_dict(), type_name, key=key)


class ProcessedFileMetadata(object):
    def __init__(self, uuid=None, accession=None, file_format='', lab='4dn-dcic-lab',
                 extra_files=None,
                 award='1U01CA200059-01', status='to be uploaded by workflow'):
        self.uuid = uuid if uuid else str(uuid4())
        self.accession = accession if accession else generate_rand_accession()
        self.status = status
        self.lab = lab
        self.award = award
        self.file_format = file_format
        if extra_files:
            self.extra_files = extra_files

    def as_dict(self):
        return self.__dict__

    def toJSON(self):
        return json.dumps(self, default=lambda o: o.__dict__, sort_keys=True, indent=4)

    def post(self, key):
        return post_to_metadata(self.as_dict(), "file_processed", key=key)


def fdn_connection(key='', connection=None, keyname='default'):
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


def authorized_request(url, auth=None, **kwargs):
    """
    Generalized request that takes the same authorization info as fdn_connection
    and is used to make request to FF.
    Takes a required url and auth, and optional headers. Any other kwargs
    provided are also past into the request.
    auth should be obtained using s3Utils.get_key or in submit_utils tuple form.
    If not provided, try to get the key using s3_utils if 'ff_env' in kwargs
    timeout of 20 seconds used by default but can be overwritten as a kwarg

    ONLY FOR GET REQUESTS.
    usage:
    authorized_request('https://data.4dnucleome.org/<some path>', (<authId, authSecret))
    OR
    authorized_request('https://data.4dnucleome.org/<some path>', ff_env='fourfront-webprod')
    """
    # first see if key should be obtained from using ff_env
    if not auth and 'ff_env' in kwargs:
        # webprod and webprod2 both use the fourfront-webprod bucket for keys
        use_env = 'fourfront-webprod' if 'webprod' in kwargs['ff_env'] else kwargs['ff_env']
        auth = s3_utils.s3Utils(env=use_env).get_key()
        del kwargs['ff_env']
    # see if auth is directly from get_key() or the tuple form used in submit_utils
    use_auth = None
    if isinstance(auth, dict) and isinstance(auth.get('default'), dict):
        use_auth = (auth['default']['key'], auth['default']['secret'])
    elif isinstance(auth, tuple) and len(auth) == 2:
        use_auth = auth
    if not use_auth:
        raise Exception("ERROR!\nInvalid authoization key %s" % auth)
    headers = kwargs.get('headers')
    if not headers:
        headers = {'content-type': 'application/json', 'accept': 'application/json'}
    if 'timeout' not in kwargs:
        kwargs['timeout'] = 20  # use a 20 second timeout by default
    return requests.get(url, auth=use_auth, **kwargs)


def search_metadata(search_url, key='', connection=None, frame="object"):
    """
    Use get_FDN, but with url_addon instead of obj_id. Will return json,
    specifically the @graph contents if available.
    """
    connection = fdn_connection(key, connection)
    return submit_utils.get_FDN(None, connection, frame=frame, url_addon=search_url)


def patch_metadata(patch_item, obj_id='', key='', connection=None):
    '''
    obj_id can be uuid or @id for most object
    '''

    connection = fdn_connection(key, connection)

    obj_id = obj_id if obj_id else patch_item['uuid']

    try:
        response = submit_utils.patch_FDN(obj_id, connection, patch_item)

        if response.get('status') == 'error':
            raise Exception("error %s \n unable to patch obj: %s \n with  data: %s" %
                            (response, obj_id, patch_item))
    except Exception as e:
        raise Exception("error %s \nunable to patch object %s \ndata: %s" % (e, obj_id, patch_item))
    return response


def get_metadata(obj_id, key='', connection=None, frame="object"):
    connection = fdn_connection(key, connection)
    res = submit_utils.get_FDN(obj_id, connection, frame=frame)
    retry = 1
    sleep = [2, 4, 12]
    while 'error' in res.get('@type', []) and retry < 3:
        time.sleep(sleep[retry])
        retry += 1
        res = submit_utils.get_FDN(obj_id, connection, frame=frame)

    return res


def post_to_metadata(post_item, schema_name, key='', connection=None):
    connection = fdn_connection(key, connection)

    try:
        response = submit_utils.new_FDN(connection, schema_name, post_item)
        if (response.get('status') == 'error' and response.get('detail') == 'UUID conflict'):
            # item already posted lets patch instead
            response = patch_metadata(post_item, connection=connection)
        elif response.get('status') == 'error':
            raise Exception("error %s \n unable to post data to schema %s, data: %s" %
                            (response, schema_name, post_item))
    except Exception as e:
        raise Exception("error %s \nunable to post data to schema %s, data: %s" %
                        (e, schema_name, post_item))
    return response


def delete_field(post_json, del_field, connection=None):
    """Does a put to delete the given field."""
    my_uuid = post_json.get("uuid")
    my_accession = post_json.get("accesion")
    raw_json = submit_utils.get_FDN(my_uuid, connection, frame="raw")
    # check if the uuid is in the raw_json
    if not raw_json.get("uuid"):
        raw_json["uuid"] = my_uuid
    # if there is an accession, add it to raw so it does not created again
    if my_accession:
        if not raw_json.get("accession"):
            raw_json["accession"] = my_accession
    # remove field from the raw_json
    if raw_json.get(del_field):
        del raw_json[del_field]
    # Do the put with raw_json
    try:
        response = submit_utils.put_FDN(my_uuid, connection, raw_json)
        if response.get('status') == 'error':
            raise Exception("error %s \n unable to delete field: %s \n of  item: %s" %
                            (response, del_field, my_uuid))
    except Exception as e:
        raise Exception("error %s \n unable to delete field: %s \n of  item: %s" %
                        (e, del_field, my_uuid))
    return response


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
    # import pdb; pdb.set_trace()
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
