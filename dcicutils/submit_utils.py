#!/usr/bin/env python3
# -*- coding: latin-1 -*-

import requests
import json
import logging
import os.path


class FdnConnectionException(Exception):
    pass


class FDN_Key:
    def __init__(self, keyfile, keyname):
        self.error = False
        # is the keyfile a dictionary
        if isinstance(keyfile, dict):
            keys = keyfile
        # is the keyfile a file (the expected case)
        elif os.path.isfile(str(keyfile)):
            keys_f = open(keyfile, 'r')
            keys_json_string = keys_f.read()
            keys_f.close()
            keys = json.loads(keys_json_string)
        # if both fail, the file does not exist
        else:
            print("\nThe keyfile does not exist, check the --keyfile path or add 'keypairs.json' to your home folder\n")
            self.error = True
            return
        key_dict = keys[keyname]
        self.authid = key_dict['key']
        self.authpw = key_dict['secret']
        self.server = key_dict['server']
        if not self.server.endswith("/"):
            self.server += "/"


class FDN_Connection(object):

    def set_award(self, lab, dontPrompt=True):
        '''Sets the award for the connection for use in import_data
           if dontPrompt is False will ask the User to choose if there
           are more than one award for the connection.lab otherwise
           the first award for the lab will be used
        '''
        lab_url = self.server + lab + '?frame=embedded'
        lab_resp = requests.get(lab_url, auth=self.auth)
        try:
            labjson = lab_resp.json()
            if labjson.get('awards') is not None:
                awards = labjson.get('awards')
                if dontPrompt:
                    self.award = awards[0]['@id']
                    return
                else:
                    if len(awards) == 1:
                        self.award = awards[0]['@id']
                        return
                    else:
                        achoices = []
                        print("Multiple awards for {labname}:".format(labname=lab))
                        for i, awd in enumerate(awards):
                            ch = str(i + 1)
                            achoices.append(ch)
                            print("  ({choice}) {awdname}".format(choice=ch, awdname=awd['@id']))
                        awd_resp = str(input("Select the award for this session {choices}: ".format(choices=achoices)))
                        if awd_resp not in achoices:
                            print("Not a valid choice - using {default}".format(default=awards[0]['@id']))
                            return
                        else:
                            self.award = awards[int(awd_resp) - 1]['@id']
                            return
            else:
                self.award = None
        except:  # noqa
            if not self.award:  # only reset if not already set
                self.award = None

    def __init__(self, key):
        self.headers = {'content-type': 'application/json', 'accept': 'application/json'}
        self.server = key.server
        if (key.authid, key.authpw) == ("", ""):
            self.auth = ()
        else:
            self.auth = (key.authid, key.authpw)
        self.check = False
        # check connection and find user uuid
        me_page = self.server + 'me' + '?frame=embedded'
        r = requests.get(me_page, auth=self.auth)
        if r.status_code == 307:
            self.check = True
            res = r.json()
            self.user = res['@id']
            self.email = res['email']

            if res.get('submits_for') is not None:
                # get all the labs that the user making the connection submits_for
                self.labs = [l['link_id'].replace("~", "/") for l in res['submits_for']]
                # take the first one as default value for the connection - reset in
                # import_data if needed by calling set_lab_award
                self.lab = self.labs[0]
                self.set_award(self.lab)  # set as default first
            else:
                self.labs = None
                self.lab = None
                self.award = None

    def prompt_for_lab_award(self):
        '''Check to see if user submits_for multiple labs or the lab
            has multiple awards and if so prompts for the one to set
            for the connection
        '''
        if self.labs:
            if len(self.labs) > 1:
                lchoices = []
                print("Submitting for multiple labs:")
                for i, lab in enumerate(self.labs):
                    ch = str(i + 1)
                    lchoices.append(ch)
                    print("  ({choice}) {labname}".format(choice=ch, labname=lab))
                lab_resp = str(input("Select the lab for this connection {choices}: ".format(choices=lchoices)))
                if lab_resp not in lchoices:
                    print("Not a valid choice - using {default}".format(default=self.lab))
                    return
                else:
                    self.lab = self.labs[int(lab_resp) - 1]

        self.set_award(self.lab, False)


class FDN_Schema(object):
    def __init__(self, connection, uri):
        self.uri = uri
        self.connection = connection
        self.server = connection.server
        response = get_FDN(uri, connection)
        self.properties = response['properties']
        self.required = None
        if 'required' in response:
            self.required = response['required']


def FDN_url(obj_id, connection, frame, url_addon=None):
    '''Generate a URL from connection info for a specific item by using an
        object id (accession, uuid or unique_key) or for a collection of items
        using the collection name (eg. biosamples or experiments-hi-c) or a
        search by providing a search suffix addon (eg. search/?type=OntologyTerm).
    '''
    if obj_id is not None:
        if frame is None:
            if '?' in obj_id:
                url = connection.server + obj_id + '&limit=all'
            else:
                url = connection.server + obj_id + '?limit=all'
        elif '?' in obj_id:
            url = connection.server + obj_id + '&limit=all&frame=' + frame
        else:
            url = connection.server + obj_id + '?limit=all&frame=' + frame
        return url
    elif url_addon is not None:  # pragma: no cover
        return connection.server + url_addon


def format_to_json(input_data):
    json_payload = {}
    if isinstance(input_data, dict):
        json_payload = json.dumps(input_data)
    elif isinstance(input_data, str):
        json_payload = input_data
    else:  # pragma: no cover
        print('Datatype is not string or dict. (format_to_json)')
    return json_payload


def get_FDN(obj_id, connection, frame="object", url_addon=None):
    '''GET an FDN object, collection or search result as JSON and
        return as dict or list of dicts for objects, and collection
        or search, respectively.
        Since we check if an object exists with this method, the logging is disabled for 404.
    '''
    if obj_id is not None:
        url = FDN_url(obj_id, connection, frame)
    elif url_addon is not None:
        url = FDN_url(None, connection, None, url_addon)
    response = requests.get(url, auth=connection.auth, headers=connection.headers)
    if response.status_code not in [200, 404]:  # pragma: no cover
        try:
            logging.warning('%s' % (response.json().get("notification")))
        except:  # noqa
            logging.warning('%s' % (response.text))
    if url_addon and response.json().get('@graph'):  # pragma: no cover
        return response.json()['@graph']
    return response.json()


def search_FDN(sheet, field, value, connection):
    '''When there is a conflict in a field that should be unique, pass
    sheet, field, unique value, and find the already exisint object.
    '''
    obj_id = "search/?type={sheet}&{field}={value}".format(sheet=sheet, field=field, value=value)
    url = FDN_url(obj_id, connection, frame="object")
    response = requests.get(url, auth=connection.auth, headers=connection.headers)
    if not response.status_code == 200:  # pragma: no cover
        try:
            logging.warning('%s' % (response.json().get("notification")))
        except:  # noqa
            logging.warning('%s' % (response.text))
    if response.json().get('@graph'):
        return response.json()['@graph']
    return response.json()


def patch_FDN(obj_id, connection, patch_input, url_addon=None):
    '''PATCH an existing FDN object and return the response JSON
    '''
    json_payload = format_to_json(patch_input)
    url = connection.server + obj_id
    if url_addon:
        url = url + url_addon
    response = requests.patch(url, auth=connection.auth, data=json_payload, headers=connection.headers)
    if not response.status_code == 200:  # pragma: no cover
        try:
            logging.debug('%s' % (response.json().get("notification")))
        except:  # noqa
            logging.debug('%s' % (response.text))
    return response.json()


def put_FDN(obj_id, connection, put_input):
    '''PUT an existing FDN object and return the response JSON'''
    json_payload = format_to_json(put_input)
    url = connection.server + obj_id
    response = requests.put(url, auth=connection.auth, data=json_payload, headers=connection.headers)
    if not response.status_code == 200:  # pragma: no cover
        try:
            logging.debug('%s' % (response.json().get("notification")))
        except:  # noqa
            logging.debug('%s' % (response.text))
    return response.json()


def new_FDN(connection, collection_name, post_input, url_addon=None):
    '''POST an FDN object as JSON and return the response JSON'''
    json_payload = format_to_json(post_input)
    url = connection.server + collection_name
    if url_addon:
        url = url + url_addon
    response = requests.post(url, auth=connection.auth, headers=connection.headers, data=json_payload)
    if not response.status_code == 201:  # pragma: no cover
        try:
            logging.debug('%s' % (response.json().get("notification")))
        except:  # noqa
            logging.debug('%s' % (response.text))
    return response.json()


def new_FDN_check(connection, collection_name, post_input):
    '''Test POST an FDN object as JSON and return the response JSON'''
    json_payload = format_to_json(post_input)
    url = connection.server + collection_name + "/?check_only=True"
    response = requests.post(url, auth=connection.auth, headers=connection.headers, data=json_payload)
    return response.json()


def patch_FDN_check(obj_id, connection, patch_input):
    '''Test PATCH an existing FDN object and return the response JSON'''
    json_payload = format_to_json(patch_input)
    url = connection.server + obj_id + "/?check_only=True"
    response = requests.patch(url, auth=connection.auth, data=json_payload, headers=connection.headers)
    return response.json()
