from __future__ import print_function
import json
import boto3
import os
import mimetypes
from zipfile import ZipFile
from io import BytesIO
import logging


###########################
# Config
###########################
LOG = logging.getLogger(__name__)


class s3Utils(object):

    def __init__(self, outfile_bucket=None, sys_bucket=None, raw_file_bucket=None, env=None):
        '''
        if we pass in env set the outfile and sys bucket from the environment
        '''

        self.url = ''
        self.s3 = boto3.client('s3', region_name='us-east-1')
        # avoid circular ref, import as needed
        from dcicutils import beanstalk_utils as bs
        if sys_bucket is None:
            # staging and production share same buckets
            if env:
                if 'webprod' in env or env in ['staging', 'stagging', 'data']:
                    self.url = bs.get_beanstalk_real_url(env)
                    env = 'fourfront-webprod'
            # we use standardized naming schema, so s3 buckets always have same prefix
            sys_bucket = "elasticbeanstalk-%s-system" % env
            outfile_bucket = "elasticbeanstalk-%s-wfoutput" % env
            raw_file_bucket = "elasticbeanstalk-%s-files" % env
            blob_bucket = "elasticbeanstalk-%s-blobs" % env

        self.sys_bucket = sys_bucket
        self.outfile_bucket = outfile_bucket
        self.raw_file_bucket = raw_file_bucket
        self.blob_bucket = blob_bucket

    def get_access_keys(self):
        name = 'illnevertell'
        keys = self.get_key(keyfile_name=name)

        if isinstance(keys.get('default'), dict):
            keys = keys['default']
        if self.url:
            keys['server'] = self.url
        return keys

    def get_ff_key(self):
        return self.get_access_keys()

    def get_higlass_key(self):
        return self.get_key(keyfile_name='hiwillnevertell')

    def get_google_key(self):
        return self.get_key(keyfile_name='fourdn-fourfront-google-key')

    def get_key(self, keyfile_name='illnevertell'):
        # Share secret encrypted S3 File
        response = self.s3.get_object(Bucket=self.sys_bucket,
                                      Key=keyfile_name,
                                      SSECustomerKey=os.environ.get("SECRET"),
                                      SSECustomerAlgorithm='AES256')
        akey = response['Body'].read()
        if type(akey) == bytes:
            akey = akey.decode()
        try:
            return json.loads(akey)
        except (ValueError, TypeError):
            # maybe its not json after all
            return akey

    def read_s3(self, filename):
        response = self.s3.get_object(Bucket=self.outfile_bucket, Key=filename)
        LOG.info(str(response))
        return response['Body'].read()

    def does_key_exist(self, key, bucket=None, print_error=True):
        if not bucket:
            bucket = self.outfile_bucket
        try:
            file_metadata = self.s3.head_object(Bucket=bucket, Key=key)
        except Exception as e:
            if print_error:
                print("object %s not found on bucket %s" % (str(key), str(bucket)))
                print(str(e))
            return False
        return file_metadata

    def get_file_size(self, key, bucket=None, add_bytes=0, add_gb=0,
                      size_in_gb=False):
        '''
        default returns file size in bytes,
        unless size_in_gb = True
        '''
        meta = self.does_key_exist(key, bucket)
        if not meta:
            raise Exception("key not found")
        one_gb = 1073741824
        add = add_bytes + (add_gb * one_gb)
        size = meta['ContentLength'] + add
        if size_in_gb:
            size = size / one_gb
        return size

    def delete_key(self, key, bucket=None):
        if not bucket:
            bucket = self.outfile_bucket
        self.s3.delete_object(Bucket=bucket, Key=key)

    def size(self, bucket):
        sbuck = boto3.resource('s3').Bucket(bucket)
        # get only head of objects so we can count them
        return sum(1 for _ in sbuck.objects.all())

    def s3_put(self, obj, upload_key, acl=None):
        '''
        try to guess content type
        '''
        content_type = mimetypes.guess_type(upload_key)[0]
        if content_type is None:
            content_type = 'binary/octet-stream'
        if acl:
            # we use this to set some of the object as public
            return self.s3.put_object(Bucket=self.outfile_bucket,
                                      Key=upload_key,
                                      Body=obj,
                                      ContentType=content_type,
                                      ACL=acl
                                      )
        else:
            return self.s3.put_object(Bucket=self.outfile_bucket,
                                      Key=upload_key,
                                      Body=obj,
                                      ContentType=content_type
                                      )

    def s3_put_secret(self, data, keyname, bucket=None, secret=None):
        if not bucket:
            bucket = self.sys_bucket
        if secret is None:
            secret = os.environ.get("SECRET")
            if secret is None:
                raise RuntimeError("SECRET should be defined in env")
        return self.s3.put_object(Bucket=bucket,
                                  Key=keyname,
                                  Body=data,
                                  SSECustomerKey=secret,
                                  SSECustomerAlgorithm='AES256')

    def s3_read_dir(self, prefix):
        return self.s3.list_objects(Bucket=self.outfile_bucket, Prefix=prefix)

    def s3_delete_dir(self, prefix):
        # one query get list of all the files we want to delete
        obj_list = self.s3.list_objects(Bucket=self.outfile_bucket, Prefix=prefix)
        files = obj_list.get('Contents', [])

        # morph file list into format that boto3 wants
        delete_keys = {'Objects': []}
        delete_keys['Objects'] = [{'Key': k} for k in
                                  [obj['Key'] for obj in files]]

        # second query deletes all the files, NOTE: Max 1000 files
        if delete_keys['Objects']:
            self.s3.delete_objects(Bucket=self.outfile_bucket, Delete=delete_keys)

    def read_s3_zipfile(self, s3key, files_to_extract):
        s3_stream = self.read_s3(s3key)
        bytestream = BytesIO(s3_stream)
        zipstream = ZipFile(bytestream, 'r')
        ret_files = {}

        for name in files_to_extract:
            # search subdirectories for file with name
            # so I don't have to worry about figuring out the subdirs
            zipped_filename = find_file(name, zipstream)
            if zipped_filename:
                ret_files[name] = zipstream.open(zipped_filename).read()
        return ret_files

    def unzip_s3_to_s3(self, zipped_s3key, dest_dir, retfile_names=None, acl=None):
        if retfile_names is None:
            retfile_names = []

        if not dest_dir.endswith('/'):
            dest_dir += '/'

        s3_stream = self.read_s3(zipped_s3key)
        # read this badboy to memory, don't go to disk
        bytestream = BytesIO(s3_stream)
        zipstream = ZipFile(bytestream, 'r')

        # directory should be first name in the list
        file_list = zipstream.namelist()
        basedir_name = file_list.pop(0)
        assert basedir_name.endswith('/')

        ret_files = {}
        for file_name in file_list:
            # don't copy dirs just files
            if not file_name.endswith('/'):
                s3_file_name = file_name.replace(basedir_name, dest_dir)
                s3_key = "https://s3.amazonaws.com/%s/%s" % (self.outfile_bucket, s3_file_name)
                # just perf optimization so we don't have to copy
                # files twice that we want to further interogate
                the_file = zipstream.open(file_name, 'r').read()
                file_to_find = file_name.split('/')[-1]
                if file_to_find in retfile_names:
                    ret_files[file_to_find] = {'s3key': s3_key,
                                               'data': the_file}
                self.s3_put(the_file, s3_file_name, acl=acl)

        return ret_files


def find_file(name, zipstream):
    for zipped_filename in zipstream.namelist():
        if zipped_filename.endswith(name):
            return zipped_filename
