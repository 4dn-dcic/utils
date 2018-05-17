from __future__ import print_function
from dcicutils import ff_utils
import datetime
from uuid import uuid4
import json


######################################
# Tibanna-related metadata functions #
######################################


def create_ffmeta_awsem(workflow, app_name, input_files=None,
                        parameters=None, title=None, uuid=None,
                        output_files=None, award='1U01CA200059-01', lab='4dn-dcic-lab',
                        run_status='started', run_platform='AWSEM', run_url='', tag=None,
                        aliases=None, awsem_postrun_json=None, submitted_by=None, extra_meta=None,
                        **kwargs):

    input_files = [] if input_files is None else input_files
    parameters = [] if parameters is None else parameters
    if award is None:
        award = '1U01CA200059-01'
    if lab is None:
        lab = '4dn-dcic-lab'

    if title is None:
        if tag is None:
            title = app_name + " run " + str(datetime.datetime.now())
        else:
            title = app_name + ' ' + tag + " run " + str(datetime.datetime.now())

    return WorkflowRunMetadata(workflow=workflow, app_name=app_name, input_files=input_files,
                               parameters=parameters, uuid=uuid, award=award,
                               lab=lab, run_platform=run_platform, run_url=run_url,
                               title=title, output_files=output_files, run_status=run_status,
                               aliases=aliases, awsem_postrun_json=awsem_postrun_json,
                               submitted_by=submitted_by, extra_meta=extra_meta)


class WorkflowRunMetadata(object):
    '''
    fourfront metadata
    '''

    def __init__(self, workflow, app_name, input_files=[],
                 parameters=[], uuid=None,
                 award='1U01CA200059-01', lab='4dn-dcic-lab',
                 run_platform='AWSEM', title=None, output_files=None,
                 run_status='started', awsem_job_id=None,
                 run_url='', aliases=None, awsem_postrun_json=None,
                 submitted_by=None, extra_meta=None, **kwargs):
        """Class for WorkflowRun that matches the 4DN Metadata schema
        Workflow (uuid of the workflow to run) has to be given.
        Workflow_run uuid is auto-generated when the object is created.
        """
        if run_platform == 'AWSEM':
            self.awsem_app_name = app_name
            # self.app_name = app_name
            if awsem_job_id is None:
                self.awsem_job_id = ''
            else:
                self.awsem_job_id = awsem_job_id
        else:
            raise Exception("invalid run_platform {} - it must be AWSEM".format(run_platform))

        self.run_status = run_status
        self.uuid = uuid if uuid else str(uuid4())
        self.workflow = workflow
        self.run_platform = run_platform
        if run_url:
            self.run_url = run_url

        self.title = title
        if aliases:
            if isinstance(aliases, basestring):  # noqa
                aliases = [aliases, ]
            self.aliases = aliases
        self.input_files = input_files
        if output_files:
            self.output_files = output_files
        self.parameters = parameters
        self.award = award
        self.lab = lab
        if awsem_postrun_json:
            self.awsem_postrun_json = awsem_postrun_json
        if submitted_by:
            self.submitted_by = submitted_by

        if extra_meta:
            for k, v in extra_meta.iteritems():
                self.__dict__[k] = v

    def append_outputfile(self, outjson):
        self.output_files.append(outjson)

    def as_dict(self):
        return self.__dict__

    def toJSON(self):
        return json.dumps(self, default=lambda o: o.__dict__, sort_keys=True, indent=4)

    def post(self, key, type_name=None):
        if not type_name:
            if self.run_platform == 'AWSEM':
                type_name = 'workflow_run_awsem'
            else:
                raise Exception("cannot determine workflow schema type from the run platform: should be AWSEM.")
        return ff_utils.post_metadata(self.as_dict(), type_name, key=key)


class ProcessedFileMetadata(object):
    def __init__(self, uuid=None, accession=None, file_format='', lab='4dn-dcic-lab',
                 extra_files=None, source_experiments=None,
                 award='1U01CA200059-01', status='to be uploaded by workflow',
                 md5sum=None, file_size=None,
                 **kwargs):
        self.uuid = uuid if uuid else str(uuid4())
        self.accession = accession if accession else ff_utils.generate_rand_accession()
        self.status = status
        self.lab = lab
        self.award = award
        self.file_format = file_format
        if extra_files:
            self.extra_files = extra_files
        if source_experiments:
            self.source_experiments = source_experiments
        if md5sum:
            self.md5sum = md5sum
        if file_size:
            self.file_size = file_size

    def as_dict(self):
        return self.__dict__

    def toJSON(self):
        return json.dumps(self, default=lambda o: o.__dict__, sort_keys=True, indent=4)

    def post(self, key):
        return ff_utils.post_metadata(self.as_dict(), "file_processed", key=key, add_on='?force_md5')

    @classmethod
    def get(cls, uuid, key, return_data=False):
        data = ff_utils.get_metadata(uuid, key=key)
        if type(data) is not dict:
            raise Exception("unable to find object with unique key of %s" % uuid)
        if 'FileProcessed' not in data.get('@type', {}):
            raise Exception("you can only load ProcessedFiles into this object")

        pf = ProcessedFileMetadata(**data)
        if return_data:
            return pf, data
        else:
            return pf

######################################
# Tibanna-related utility functions #
######################################


def aslist(x):
    """
    From tibanna
    """
    if isinstance(x, list):
        return x
    else:
        return [x]


def ensure_list(val):
    """
    From tibanna
    """
    if isinstance(val, (list, tuple)):
        return val
    return [val]


def get_extra_file_key(infile_format, infile_key, extra_file_format, fe_map):
    """
    From tibanna
    """
    infile_extension = fe_map.get(infile_format)
    extra_file_extension = fe_map.get(extra_file_format)
    return infile_key.replace(infile_extension, extra_file_extension)


def get_source_experiment(input_file_uuid, ff_keys):
    """
    Connects to fourfront and get source experiment info as a unique list
    Takes a single input file uuid.
    From tibanna
    """
    pf_source_experiments_set = set()
    inf_uuids = aslist(input_file_uuid)
    for inf_uuid in inf_uuids:
        infile_meta = ff_utils.get_metadata(inf_uuid, key=ff_keys)
        if infile_meta.get('experiments'):
            for exp in infile_meta.get('experiments'):
                exp_uuid = ff_utils.get_metadata(exp, key=ff_keys).get('uuid')
                pf_source_experiments_set.add(exp_uuid)
        if infile_meta.get('source_experiments'):
            pf_source_experiments_set.update(infile_meta.get('source_experiments'))
    return list(pf_source_experiments_set)


def merge_source_experiments(input_file_uuids, ff_keys):
    """
    Connects to fourfront and get source experiment info as a unique list
    Takes a list of input file uuids.
    From tibanna
    """
    pf_source_experiments = set()
    for input_file_uuid in input_file_uuids:
        pf_source_experiments.update(get_source_experiment(input_file_uuid, ff_keys))
    return list(pf_source_experiments)


def get_format_extension_map(ff_keys):
    """
    get format-extension map
    From tibanna
    """
    try:
        fp_schema = ff_utils.get_metadata("profiles/file_processed.json", key=ff_keys)
        fe_map = fp_schema.get('file_format_file_extension')
    except Exception as e:
        raise Exception("Can't get format-extension map from file_processed schema. %s\n" % e)
    return fe_map
