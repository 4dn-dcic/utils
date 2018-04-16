# flake8: noqa
import pytest
import dcicutils.submit_utils as submit_utils


class MockedResponse(object):
    def __init__(self, json, status):
        self._json = json
        self.status_code = status

    def json(self):
        return self._json


@pytest.fixture
def connection():
    keypairs = {
                "default":
                {"server": "https://data.4dnucleome.org/",
                 "key": "testkey",
                 "secret": "testsecret"
                 }
                }
    key = submit_utils.FDN_Key(keypairs, "default")
    connection = submit_utils.FDN_Connection(key)
    connection.lab = 'test_lab'
    connection.user = 'test_user'
    connection.award = 'test_award'
    return connection


@pytest.fixture
def connection_public():
    keypairs = {
                "default":
                {"server": "https://data.4dnucleome.org/",
                 "key": "",
                 "secret": ""
                 }
                }
    key2 = submit_utils.FDN_Key(keypairs, "default")
    connection = submit_utils.FDN_Connection(key2)
    connection.lab = 'test_lab'
    connection.user = 'test_user'
    connection.award = 'test_award'
    return connection


@pytest.fixture
def connection_fake():
    keypairs = {
                "default":
                {"server": "https://data.4dnucleome.org/",
                 "key": "",
                 "secret": ""
                 }
                }
    key2 = submit_utils.FDN_Key(keypairs, "default")
    connection = submit_utils.FDN_Connection(key2)
    connection.lab = 'test_lab'
    connection.user = 'test_user'
    connection.award = 'test_award'
    connection.email = 'test@test.test'
    connection.labs = ['test_lab']
    connection.check = True
    return connection


@pytest.fixture(scope="module")
def item_properties():
    return {'@id': {'calculatedProperty': True, 'title': 'ID', 'type': 'string'},
            '@type': {'calculatedProperty': True,
                      'items': {'type': 'string'},
                      'title': 'Type',
                      'type': 'array'},
            'description': {'rdfs:subPropertyOf': 'dc:description',
                            'title': 'Description',
                            'type': 'string'},
            "experiment_sets": {"type": "array",
                                "description": "Experiment Sets that are associated with this experiment.",
                                "title": "Experiment Sets",
                                "items": {
                                    "type": "string",
                                    "description": "An experiment set that is associated wtih this experiment.",
                                    "linkTo": "ExperimentSet",
                                    "title": "Experiment Set"},
                                "uniqueItems": True},
            'end_date': {'anyOf': [{'format': 'date-time'}, {'format': 'date'}],
                         'comment': 'Date can be submitted as YYYY-MM-DD or '
                         'YYYY-MM-DDTHH:MM:SSTZD (TZD is the time zone '
                         'designator; use Z to express time in UTC or for time '
                         'expressed in local time add a time zone offset from '
                         'UTC +HH:MM or -HH:MM).',
                         'title': 'End date',
                         'type': 'string'},
            'name': {'description': 'The official grant number from the NIH database, if '
                     'applicable',
                     'pattern': '^[A-Za-z0-9\\-]+$',
                     'title': 'Number',
                     'type': 'string',
                     'uniqueKey': True},
            'pi': {'comment': 'See user.json for available identifiers.',
                   'description': 'Principle Investigator of the grant.',
                   'linkTo': 'User',
                   'title': 'P.I.',
                   'type': 'string'},
            'project': {'description': 'The name of the consortium project',
                        'enum': ['4DN', 'External'],
                        'title': 'Project',
                        'type': 'string'},
            'schema_version': {'comment': 'Do not submit, value is assigned by the '
                               'server. The version of the JSON schema that '
                               'the server uses to validate the object. Schema '
                               'version indicates generation of schema used to '
                               'save version to to enable upgrade steps to '
                               'work. Individual schemas should set the '
                               'default.',
                               'default': '1',
                               'pattern': '^\\d+(\\.\\d+)*$',
                               'requestMethod': [],
                               'title': 'Schema Version',
                               'type': 'string'},
            'start_date': {'anyOf': [{'format': 'date-time'}, {'format': 'date'}],
                           'comment': 'Date can be submitted as YYYY-MM-DD or '
                           'YYYY-MM-DDTHH:MM:SSTZD (TZD is the time zone '
                           'designator; use Z to express time in UTC or for '
                           'time expressed in local time add a time zone '
                           'offset from UTC +HH:MM or -HH:MM).',
                           'title': 'Start date',
                           'type': 'string'},
            'status': {'default': 'current',
                       'enum': ['current',
                                'in progress',
                                'deleted',
                                'replaced',
                                'released',
                                'revoked'],
                       'title': 'Status',
                       'type': 'string'},
            'title': {'description': 'The grant name from the NIH database, if '
                      'applicable.',
                      'rdfs:subPropertyOf': 'dc:title',
                      'title': 'Name',
                      'type': 'string'},
            'url': {'@type': '@id',
                    'description': 'An external resource with additional information '
                    'about the grant.',
                    'format': 'uri',
                    'rdfs:subPropertyOf': 'rdfs:seeAlso',
                    'title': 'URL',
                    'type': 'string'},
            'uuid': {'format': 'uuid',
                     'requestMethod': 'POST',
                     'serverDefault': 'uuid4',
                     'title': 'UUID',
                     'type': 'string'},
            'viewing_group': {'description': 'The group that determines which set of data '
                              'the user has permission to view.',
                              'enum': ['4DN', 'Not 4DN'],
                              'title': 'View access group',
                              'type': 'string'}}


@pytest.fixture
def calc_properties():
    return {'@id': {'calculatedProperty': True, 'title': 'ID', 'type':
                    'string'},
            '@type': {'calculatedProperty': True,
                      'items': {'type': 'string'},
                      'title': 'Type',
                      'type': 'array'},
            'description': {'rdfs:subPropertyOf': 'dc:description',
                            'title': 'Description',
                            'type': 'string'},
            }


@pytest.fixture
def embed_properties():
    return {'experiment_relation': {'description': 'All related experiments',
                                    'items': {'additionalProperties': False,
                                              'properties':
                                              {'experiment': {'description': 'The '
                                                                             'related '
                                                                             'experiment',
                                                              'linkTo': 'Experiment',
                                                              'type': 'string'},
                                               'relationship_type': {'description': 'A '
                                                                     'controlled '
                                                                     'term '
                                                                     'specifying '
                                                                     'the '
                                                                     'relationship '
                                                                     'between '
                                                                     'experiments.',
                                                                     'enum': ['controlled '
                                                                              'by',
                                                                              'control '
                                                                              'for',
                                                                              'derived '
                                                                              'from',
                                                                              'source '
                                                                              'for'],
                                                                     'title': 'Relationship '
                                                                              'Type',
                                                                              'type': 'string'}},
                                              'title': 'Experiment relation',
                                              'type': 'object'},
                                    'title': 'Experiment relations',
                                    'type': 'array'},
            }


@pytest.fixture
def file_metadata():
    from collections import OrderedDict
    return OrderedDict([('aliases', 'dcic:HIC00test2'),
                        ('award', '/awards/OD008540-01/'),
                        ('file_classification', 'raw file'),
                        ('file_format', 'fastq'),
                        ('filesets', ''),
                        ('instrument', 'Illumina HiSeq 2000'),
                        ('lab', '/labs/erez-liebermanaiden-lab/'),
                        ('paired_end', ''),
                        ('related_files.file', 'testfile.fastq'),
                        ('related_files.relationship_type', 'related_to'),
                        ('experiment_relation.experiment', 'test:exp002'),
                        ('experiment_relation.relationship_type', 'controlled by'),
                        ('experiment_relation.experiment-1', 'test:exp003'),
                        ('experiment_relation.relationship_type-1', 'source for'),
                        ('experiment_relation.experiment-2', 'test:exp004'),
                        ('experiment_relation.relationship_type-2', 'source for'),
                        ('status', 'uploaded')])


@pytest.fixture
def file_metadata_type():
    return {'aliases': 'array',
            'award': 'string',
            'file_classification': 'string',
            'file_format': 'string',
            'filesets': 'array',
            'instrument': 'string',
            'lab': 'string',
            'paired_end': 'string',
            'related_files.file': 'array',
            'related_files.relationship_type': 'array',
            'experiment_relation.experiment': 'array',
            'experiment_relation.relationship_type': 'array',
            'experiment_relation.experiment-1': 'array',
            'experiment_relation.relationship_type-1': 'array',
            'experiment_relation.experiment-2': 'array',
            'experiment_relation.relationship_type-2': 'array',
            'status': 'string'}


@pytest.fixture
def returned_award_schema():
    data = {"title":"Grant","id":"/profiles/award.json","$schema":"http://json-schema.org/draft-04/schema#","required":["name"],"identifyingProperties":["uuid","name","title"],"additionalProperties":False,"mixinProperties":[{"$ref":"mixins.json#/schema_version"},{"$ref":"mixins.json#/uuid"},{"$ref":"mixins.json#/submitted"},{"$ref":"mixins.json#/status"}],"type":"object","properties":{"status":{"readonly":True,"type":"string","default":"released","enum":["released","current","revoked","deleted","replaced","in review by lab","in review by project","released to project"],"title":"Status","permission":"import_items"},"submitted_by":{"readonly":True,"type":"string","serverDefault":"userid","linkTo":"User","comment":"Do not submit, value is assigned by the server. The user that created the object.","title":"Submitted by","rdfs:subPropertyOf":"dc:creator","permission":"import_items"},"date_created":{"readonly":True,"type":"string","serverDefault":"now","anyOf":[{"format":"date-time"},{"format":"date"}],"comment":"Do not submit, value is assigned by the server. The date the object is created.","title":"Date created","rdfs:subPropertyOf":"dc:created","permission":"import_items"},"uuid":{"requestMethod":"POST","readonly":True,"type":"string","serverDefault":"uuid4","format":"uuid","title":"UUID","permission":"import_items"},"schema_version":{"requestMethod":[],"type":"string","default":"1","pattern":"^\\d+(\\.\\d+)*$","comment":"Do not submit, value is assigned by the server. The version of the JSON schema that the server uses to validate the object. Schema version indicates generation of schema used to save version to to enable upgrade steps to work. Individual schemas should set the default.","title":"Schema Version"},"title":{"description":"The grant name from the NIH database, if applicable.","type":"string","title":"Name","rdfs:subPropertyOf":"dc:title"},"name":{"description":"The official grant number from the NIH database, if applicable","uniqueKey":True,"type":"string","title":"Number","pattern":"^[A-Za-z0-9\\-]+$"},"description":{"type":"string","title":"Description","rdfs:subPropertyOf":"dc:description"},"start_date":{"anyOf":[{"format":"date-time"},{"format":"date"}],"comment":"Date can be submitted as YYYY-MM-DD or YYYY-MM-DDTHH:MM:SSTZD (TZD is the time zone designator; use Z to express time in UTC or for time expressed in local time add a time zone offset from UTC +HH:MM or -HH:MM).","type":"string","title":"Start date"},"end_date":{"anyOf":[{"format":"date-time"},{"format":"date"}],"comment":"Date can be submitted as YYYY-MM-DD or YYYY-MM-DDTHH:MM:SSTZD (TZD is the time zone designator; use Z to express time in UTC or for time expressed in local time add a time zone offset from UTC +HH:MM or -HH:MM).","type":"string","title":"End date"},"url":{"format":"uri","type":"string","@type":"@id","description":"An external resource with additional information about the grant.","title":"URL","rdfs:subPropertyOf":"rdfs:seeAlso"},"pi":{"description":"Principle Investigator of the grant.","comment":"See user.json for available identifiers.","type":"string","title":"P.I.","linkTo":"User"},"project":{"description":"The name of the consortium project","type":"string","title":"Project","enum":["4DN","External"]},"viewing_group":{"description":"The group that determines which set of data the user has permission to view.","type":"string","title":"View access group","enum":["4DN","Not 4DN"]},"@id":{"calculatedProperty":True,"type":"string","title":"ID"},"@type":{"calculatedProperty":True,"title":"Type","type":"array","items":{"type":"string"}}},"boost_values":{"name":1,"title":1,"pi.title":1},"@type":["JSONSchema"]}
    return MockedResponse(data, 200)


@pytest.fixture
def returned_vendor_schema():
    data = {"title":"Vendor","description":"Schema for submitting an originating lab or vendor.","id":"/profiles/vendor.json","$schema":"http://json-schema.org/draft-04/schema#","type":"object","required":["title"],"identifyingProperties":["uuid","name"],"additionalProperties":False,"mixinProperties":[{"$ref":"mixins.json#/schema_version"},{"$ref":"mixins.json#/uuid"},{"$ref":"mixins.json#/status"},{"$ref":"mixins.json#/notes"},{"$ref":"mixins.json#/submitted"},{"$ref":"mixins.json#/attribution"},{"$ref":"mixins.json#/aliases"}],"properties":{"aliases":{"type":"array","default":[],"uniqueItems":True,"title":"Lab aliases","description":"Lab specific identifiers to reference an object.","items":{"comment":"Current convention is colon separated lab name and lab identifier. (e.g. john-doe:42).","pattern":"^\\S+:\\S+","uniqueKey":"alias","title":"Lab alias","description":"A lab specific identifier to reference an object.","type":"string"}},"award":{"comment":"See award.json for list of available identifiers.","title":"Grant","description":"Grant associated with the submission.","linkTo":"Award","type":"string"},"lab":{"description":"Lab associated with the submission.","linkSubmitsFor":True,"title":"Lab","comment":"See lab.json for list of available identifiers.","linkTo":"Lab","type":"string"},"date_created":{"anyOf":[{"format":"date-time"},{"format":"date"}],"serverDefault":"now","readonly":True,"type":"string","comment":"Do not submit, value is assigned by the server. The date the object is created.","title":"Date created","rdfs:subPropertyOf":"dc:created","permission":"import_items"},"submitted_by":{"serverDefault":"userid","readonly":True,"type":"string","comment":"Do not submit, value is assigned by the server. The user that created the object.","linkTo":"User","title":"Submitted by","rdfs:subPropertyOf":"dc:creator","permission":"import_items"},"notes":{"elasticsearch_mapping_index_type":{"title":"Field mapping index type","description":"Defines one of three types of indexing available","type":"string","enum":["analyzed","not_analyzed","no"],"default":"analyzed"},"description":"DCIC internal notes.","type":"string","title":"Notes"},"status":{"readonly":True,"default":"in review by lab","title":"Status","type":"string","enum":["released","current","revoked","deleted","replaced","in review by lab","in review by project","released to project"],"permission":"import_items"},"uuid":{"serverDefault":"uuid4","readonly":True,"requestMethod":"POST","type":"string","title":"UUID","format":"uuid","permission":"import_items"},"schema_version":{"default":"1","pattern":"^\\d+(\\.\\d+)*$","requestMethod":[],"title":"Schema Version","comment":"Do not submit, value is assigned by the server. The version of the JSON schema that the server uses to validate the object. Schema version indicates generation of schema used to save version to to enable upgrade steps to work. Individual schemas should set the default.","type":"string"},"description":{"title":"Description","description":"A plain text description of the source.","type":"string","default":""},"title":{"title":"Name","description":"The complete name of the originating lab or vendor. ","type":"string"},"name":{"uniqueKey":True,"type":"string","description":"DON'T SUBMIT, auto-generated, use for referencing vendors in other sheets.","pattern":"^[a-z0-9\\-]+$"},"url":{"title":"URL","description":"An external resource with additional information about the source.","type":"string","format":"uri"},"@type":{"calculatedProperty":True,"title":"Type","type":"array","items":{"type":"string"}},"@id":{"calculatedProperty":True,"title":"ID","type":"string"}},"boost_values":{"name":1,"title":1},"@type":["JSONSchema"]}
    return MockedResponse(data, 200)

@pytest.fixture
def returned_vendor_schema_l():
    data = {"title": "Vendor", "description": "Lab or Company that is the Source for a Product/Sample.", "id": "/profiles/vendor.json", "$schema": "http://json-schema.org/draft-04/schema#", "type": "object", "required": ["title", "lab", "award"], "identifyingProperties": ["uuid", "name"], "additionalProperties": False, "mixinProperties": [{"$ref": "mixins.json#/schema_version"}, {"$ref": "mixins.json#/uuid"}, {"$ref": "mixins.json#/status"}, {"$ref": "mixins.json#/notes"}, {"$ref": "mixins.json#/submitted"}, {"$ref": "mixins.json#/modified"}, {"$ref": "mixins.json#/release_dates"}, {"$ref": "mixins.json#/attribution"}, {"$ref": "mixins.json#/tags"}, {"$ref": "mixins.json#/aliases"}], "mixinFacets": [{"$ref": "mixins.json#/facets_common"}], "properties": {"aliases": {"title": "Aliases", "description": "Lab specific ID (e.g. dcic_lab:my_biosample1).", "type": "array", "comment": "Colon separated lab name and lab identifier, no slash. (e.g. dcic-lab:42).", "default": [], "lookup": 1, "uniqueItems": True, "ff_flag": "clear clone", "items": {"uniqueKey": "alias", "title": "Lab alias", "description": "Lab specific ID (e.g. dcic_lab:my_biosample1).", "type": "string", "pattern": "^[^\\s\\\\/]+:[^\\s\\\\/]+"}}, "tags": {"title": "Tags", "description": "Key words that can tag an item - useful for filtering.", "type": "array", "lookup": 1000, "uniqueItems": True, "ff_flag": "clear clone", "items": {"title": "Tag", "description": "A tag for the item.", "type": "string"}}, "lab": {"title": "Lab", "description": "Lab associated with the submission.", "exclude_from": ["submit4dn", "FFedit-create"], "type": "string", "linkTo": "Lab", "linkSubmitsFor": True, "default": ""}, "contributing_labs": {"title": "Contributing Labs", "description": "Other labs associated with the submitted data.", "type": "array", "lookup": 1000, "items": {"title": "Contributing Lab", "description": "A lab that has contributed to the associated data.", "type": "string", "linkTo": "Lab"}}, "award": {"title": "Grant", "description": "Grant associated with the submission.", "exclude_from": ["submit4dn", "FFedit-create"], "default": "", "type": "string", "linkTo": "Award"}, "public_release": {"title": "Public Release Date", "description": "The date which the item was released to the public", "comment": "Do not submit, value is assigned when released.", "type": "string", "lookup": 1000, "anyOf": [{"format": "date-time"}, {"format": "date"}], "exclude_from": ["submit4dn", "FFedit-create"], "permission": "import_items"}, "project_release": {"title": "Project Release Date", "description": "The date which the item was released to the project", "comment": "Do not submit, value is assigned when released to project.", "type": "string", "lookup": 1000, "anyOf": [{"format": "date-time"}, {"format": "date"}], "exclude_from": ["submit4dn"], "permission": "import_items"}, "last_modified": {"title": "Last Modified", "exclude_from": ["submit4dn", "FFedit-create"], "type": "object", "additionalProperties": False, "lookup": 1000, "properties": {"date_modified": {"title": "Date modified", "description": "Do not submit, value is assigned by the server. The date the object is modified.", "type": "string", "anyOf": [{"format": "date-time"}, {"format": "date"}], "permission": "import_items"}, "modified_by": {"title": "Modified by", "description": "Do not submit, value is assigned by the server. The user that modfied the object.", "type": "string", "linkTo": "User", "permission": "import_items"}}}, "date_created": {"rdfs:subPropertyOf": "dc:created", "title": "Date Created", "lookup": 1000, "exclude_from": ["submit4dn", "FFedit-create"], "type": "string", "anyOf": [{"format": "date-time"}, {"format": "date"}], "serverDefault": "now", "permission": "import_items"}, "submitted_by": {"rdfs:subPropertyOf": "dc:creator", "title": "Submitted By", "exclude_from": ["submit4dn", "FFedit-create"], "type": "string", "linkTo": "User", "lookup": 1000, "serverDefault": "userid", "permission": "import_items"}, "notes": {"title": "Notes", "description": "DCIC internal notes.", "type": "string", "exclude_from": ["submit4dn", "FFedit-create"], "elasticsearch_mapping_index_type": {"title": "Field mapping index type", "description": "Defines one of three types of indexing available", "type": "string", "default": "analyzed", "enum": ["analyzed", "not_analyzed", "no"]}}, "status": {"title": "Status", "exclude_from": ["submit4dn"], "type": "string", "default": "in review by lab", "permission": "import_items", "enum": ["released", "current", "planned", "revoked", "archived", "deleted", "obsolete", "replaced", "in review by lab", "submission in progress", "released to project", "archived to project"]}, "uuid": {"title": "UUID", "type": "string", "format": "uuid", "exclude_from": ["submit4dn", "FFedit-create"], "serverDefault": "uuid4", "permission": "import_items", "requestMethod": "POST"}, "schema_version": {"title": "Schema Version", "internal_comment": "Do not submit, value is assigned by the server. The version of the JSON schema that the server uses to validate the object. Schema version indicates generation of schema used to save version to to enable upgrade steps to work. Individual schemas should set the default.", "type": "string", "exclude_from": ["submit4dn", "FFedit-create"], "pattern": "^\\d+(\\.\\d+)*$", "requestMethod": [], "default": "1"}, "description": {"title": "Description", "description": "A plain text description of the source.", "type": "string", "lookup": 30, "default": "", "formInput": "textarea"}, "title": {"title": "Name", "description": "The complete name of the originating lab or vendor. ", "type": "string", "lookup": 20}, "name": {"description": "DON'T SUBMIT, auto-generated, use for referencing vendors in other sheets.", "type": "string", "pattern": "^[a-z0-9\\-]+$", "uniqueKey": True, "exclude_from": ["submit4dn", "FFedit-create"]}, "url": {"title": "URL", "description": "An external resource with additional information about the source.", "type": "string", "lookup": 1000, "format": "uri"}, "@id": {"title": "ID", "type": "string", "calculatedProperty": True}, "@type": {"title": "Type", "type": "array", "items": {"type": "string"}, "calculatedProperty": True}, "external_references": {"title": "External Reference URIs", "description": "External references to this item.", "type": "array", "items": {"type": "object", "title": "External Reference", "properties": {"uri": {"type": "string"}, "ref": {"type": "string"}}}, "calculatedProperty": True}, "display_title": {"title": "Display Title", "description": "A calculated title for every object in 4DN", "type": "string", "calculatedProperty": True}, "link_id": {"title": "link_id", "description": "A copy of @id that can be embedded. Uses ~ instead of /", "type": "string", "calculatedProperty": True}, "principals_allowed": {"title": "principals_allowed", "description": "calced perms for ES filtering", "type": "object", "properties": {"view": {"type": "string"}, "edit": {"type": "string"}, "audit": {"type": "string"}}, "calculatedProperty": True}}, "boost_values": {"name": 1.0, "title": 1.0}, "facets": {"award.project": {"title": "Project"}, "lab.display_title": {"title": "Lab"}}, "@type": ["JSONSchema"]}
    return MockedResponse(data, 200)

@pytest.fixture
def returned_experiment_set_schema():
    data = {"title":"Experiment set","description":"Schema for submitting metadata for an experiment set.","id":"/profiles/experiment_set.json","$schema":"http://json-schema.org/draft-04/schema#","type":"object","required":["award","lab"],"identifyingProperties":["uuid","aliases"],"additionalProperties":False,"mixinProperties":[{"$ref":"mixins.json#/schema_version"},{"$ref":"mixins.json#/accession"},{"$ref":"mixins.json#/uuid"},{"$ref":"mixins.json#/aliases"},{"$ref":"mixins.json#/status"},{"$ref":"mixins.json#/attribution"},{"$ref":"mixins.json#/submitted"},{"$ref":"mixins.json#/notes"},{"$ref":"mixins.json#/documents"}],"properties":{"documents":{"type":"array","title":"Documents","items":{"type":"string","linkTo":"Document","comment":"See document.json for available identifiers.","title":"Document","description":"A document that provides additional information (not data file)."},"default":[],"description":"Documents that provide additional information (not data file).","uniqueItems":True},"notes":{"type":"string","elasticsearch_mapping_index_type":{"type":"string","default":"analyzed","enum":["analyzed","not_analyzed","no"],"title":"Field mapping index type","description":"Defines one of three types of indexing available"},"title":"Notes","description":"DCIC internal notes."},"submitted_by":{"type":"string","linkTo":"User","comment":"Do not submit, value is assigned by the server. The user that created the object.","title":"Submitted by","rdfs:subPropertyOf":"dc:creator","readonly":True,"serverDefault":"userid","permission":"import_items"},"date_created":{"type":"string","serverDefault":"now","anyOf":[{"format":"date-time"},{"format":"date"}],"comment":"Do not submit, value is assigned by the server. The date the object is created.","title":"Date created","rdfs:subPropertyOf":"dc:created","readonly":True,"permission":"import_items"},"lab":{"type":"string","linkTo":"Lab","comment":"See lab.json for list of available identifiers.","title":"Lab","description":"Lab associated with the submission.","linkSubmitsFor":True},"award":{"type":"string","linkTo":"Award","comment":"See award.json for list of available identifiers.","title":"Grant","description":"Grant associated with the submission."},"status":{"type":"string","readonly":True,"title":"Status","enum":["released","current","revoked","deleted","replaced","in review by lab","in review by project","released to project"],"default":"in review by lab","permission":"import_items"},"aliases":{"type":"array","title":"Lab aliases","items":{"type":"string","pattern":"^\\S+:\\S+","comment":"Current convention is colon separated lab name and lab identifier. (e.g. john-doe:42).","title":"Lab alias","description":"A lab specific identifier to reference an object.","uniqueKey":"alias"},"default":[],"description":"Lab specific identifiers to reference an object.","uniqueItems":True},"uuid":{"type":"string","readonly":True,"title":"UUID","serverDefault":"uuid4","requestMethod":"POST","permission":"import_items","format":"uuid"},"accession":{"type":"string","accessionType":"ES","readonly":True,"title":"Accession","description":"A unique identifier to be used to reference the object.","serverDefault":"accession","permission":"import_items","comment":"Only admins are allowed to set or update this value.","format":"accession"},"alternate_accessions":{"type":"array","default":[],"description":"Accessions previously assigned to objects that have been merged with this object.","title":"Alternate accessions","items":{"type":"string","comment":"Only admins are allowed to set or update this value.","title":"Alternate Accession","description":"An accession previously assigned to an object that has been merged with this object.","permission":"import_items","format":"accession"}},"schema_version":{"type":"string","pattern":"^\\d+(\\.\\d+)*$","hidden comment":"Bump the default in the subclasses.","comment":"Do not submit, value is assigned by the server. The version of the JSON schema that the server uses to validate the object. Schema version indicates generation of schema used to save version to to enable upgrade steps to work. Individual schemas should set the default.","title":"Schema Version","requestMethod":[]},"experiments_in_set":{"type":"array","title":"Set of experiments","exclude_from":["submit4dn"],"default":[],"description":"List of experiments to be associatedas a set.","uniqueItems":True,"items":{"title":"Experiment","comment":"use accessions for identifiers.","type":"string","linkTo":"Experiment"}},"experimentset_type":{"type":"string","enum":["custom"],"title":"Experiment Set type","description":"The categorization of the set of experiments."},"description":{"type":"string","default":"","title":"Description","description":"A description of why experiments are part of the set."},"@type":{"type":"array","calculatedProperty":True,"title":"Type","items":{"type":"string"}},"@id":{"type":"string","calculatedProperty":True,"title":"ID"}},"facets":{"experimentset_type":{"title":"Experiment set type"},"experiments_in_set.award.project":{"title":"Project"},"experiments_in_set.biosample.biosource.individual.organism.name":{"title":"Organism"},"experiments_in_set.biosample.biosource.biosource_type":{"title":"Biosource type"},"experiments_in_set.biosample.biosource_summary":{"title":"Biosource"},"experiments_in_set.digestion_enzyme.name":{"title":"Enzyme"},"experiments_in_set.biosample.modifications_summary":{"title":"Modifications"},"experiments_in_set.biosample.treatments_summary":{"title":"Treatments"},"experiments_in_set.lab.title":{"title":"Lab"}},"columns":{"accession":{"title":"Accession"},"experimentset_type":{"title":"Experiment set type"},"description":{"title":"Description"},"experiments_in_set":{"title":"Experiments"}},"@type":["JSONSchema"]}
    return MockedResponse(data, 200)

@pytest.fixture
def returned_vendor_items():
    data = {'@id': '/search/?type=Vendor&limit=all&frame=object', 'sort': {'label': {'order': 'asc', 'missing': '_last', 'ignore_unmapped': True}, 'date_created': {'order': 'desc', 'ignore_unmapped': True}}, 'columns': {'@id': 'ID', 'aliases': 'Lab aliases', 'name': 'name', 'description': 'Description', 'title': 'Name'}, 'clear_filters': '/search/?type=Vendor', '@context': '/terms/', 'views': [{'href': '/report/?type=Vendor&limit=all&frame=object', 'title': 'View tabular report', 'icon': 'table'}], 'notification': 'Success', 'filters': [{'field': 'type', 'term': 'Vendor', 'remove': '/search/?limit=all&frame=object'}], '@type': ['Search'], '@graph': [{'url': 'https://www.thermofisher.com/us/en/home/brands/thermo-scientific.html#/legacy=www.fermentas.com', '@id': '/vendors/thermofisher-scientific/', 'aliases': [], 'status': 'in review by lab', 'description': 'previously also Fermentas', 'award': '/awards/1U01CA200059-01/', 'uuid': 'b31106bc-8535-4448-903e-854af460b21f', 'lab': '/labs/4dn-dcic-lab/', 'date_created': '2016-12-08T18:31:47.847660+00:00', '@type': ['Vendor', 'Item'], 'schema_version': '1', 'title': 'ThermoFisher Scientific', 'name': 'thermofisher-scientific', 'submitted_by': '/users/986b362f-4eb6-4a9c-8173-3ab267307e3a/'}, {'url': 'https://www.neb.com', '@id': '/vendors/new-england-biolabs/', 'aliases': [], 'status': 'in review by lab', 'description': '', 'award': '/awards/1U01CA200059-01/', 'uuid': 'b31106bc-8535-4448-903e-854af460b21e', 'lab': '/labs/4dn-dcic-lab/', 'date_created': '2016-12-08T18:31:47.824418+00:00', '@type': ['Vendor', 'Item'], 'schema_version': '1', 'title': 'New England Biolabs', 'name': 'new-england-biolabs', 'submitted_by': '/users/986b362f-4eb6-4a9c-8173-3ab267307e3a/'}, {'url': 'http://www.worthington-biochem.com', '@id': '/vendors/worthington-biochemical/', 'aliases': [], 'status': 'in review by lab', 'description': '', 'award': '/awards/1U01CA200059-01/', 'uuid': 'b31106bc-8535-4448-903e-854af460b21d', 'lab': '/labs/4dn-dcic-lab/', 'date_created': '2016-12-08T18:31:47.807726+00:00', '@type': ['Vendor', 'Item'], 'schema_version': '1', 'title': 'Worthington Biochemical', 'name': 'worthington-biochemical', 'submitted_by': '/users/986b362f-4eb6-4a9c-8173-3ab267307e3a/'}], 'title': 'Search', 'total': 3, 'facets': [{'total': 3, 'title': 'Data Type', 'field': 'type', 'terms': [{'key': 'Vendor', 'doc_count': 3}, {'key': 'AccessKey', 'doc_count': 0}, {'key': 'AnalysisStep', 'doc_count': 0}, {'key': 'Award', 'doc_count': 0}, {'key': 'Biosample', 'doc_count': 0}, {'key': 'BiosampleCellCulture', 'doc_count': 0}, {'key': 'Biosource', 'doc_count': 0}, {'key': 'Construct', 'doc_count': 0}, {'key': 'Document', 'doc_count': 0}, {'key': 'Enzyme', 'doc_count': 0}, {'key': 'Experiment', 'doc_count': 0}, {'key': 'ExperimentCaptureC', 'doc_count': 0}, {'key': 'ExperimentHiC', 'doc_count': 0}, {'key': 'ExperimentRepliseq', 'doc_count': 0}, {'key': 'File', 'doc_count': 0}, {'key': 'FileFasta', 'doc_count': 0}, {'key': 'FileFastq', 'doc_count': 0}, {'key': 'FileProcessed', 'doc_count': 0}, {'key': 'FileReference', 'doc_count': 0}, {'key': 'FileSet', 'doc_count': 0}, {'key': 'Individual', 'doc_count': 0}, {'key': 'IndividualMouse', 'doc_count': 0}, {'key': 'Lab', 'doc_count': 0}, {'key': 'Modification', 'doc_count': 0}, {'key': 'Ontology', 'doc_count': 0}, {'key': 'OntologyTerm', 'doc_count': 0}, {'key': 'Organism', 'doc_count': 0}, {'key': 'Publication', 'doc_count': 0}, {'key': 'Software', 'doc_count': 0}, {'key': 'SopMap', 'doc_count': 0}, {'key': 'Target', 'doc_count': 0}, {'key': 'Treatment', 'doc_count': 0}, {'key': 'TreatmentChemical', 'doc_count': 0}, {'key': 'TreatmentRnai', 'doc_count': 0}, {'key': 'User', 'doc_count': 0}, {'key': 'Workflow', 'doc_count': 0}, {'key': 'WorkflowRun', 'doc_count': 0}]}, {'total': 3, 'title': 'Audit category: DCC ACTION', 'field': 'audit.INTERNAL_ACTION.category', 'terms': [{'key': 'mismatched status', 'doc_count': 0}, {'key': 'validation error', 'doc_count': 0}, {'key': 'validation error: run_status', 'doc_count': 0}]}]}
    return MockedResponse(data, 200)


@pytest.fixture
def returned_vendor_item1():
    data = {'url': 'https://www.thermofisher.com/us/en/home/brands/thermo-scientific.html#/legacy=www.fermentas.com', '@id': '/vendors/thermofisher-scientific/', 'aliases': [], 'status': 'in review by lab', 'description': 'previously also Fermentas', 'award': '/awards/1U01CA200059-01/', 'uuid': 'b31106bc-8535-4448-903e-854af460b21f', 'lab': '/labs/4dn-dcic-lab/', 'date_created': '2016-12-08T18:31:47.847660+00:00', '@type': ['Vendor', 'Item'], 'schema_version': '1', 'title': 'ThermoFisher Scientific', 'name': 'thermofisher-scientific', 'submitted_by': '/users/986b362f-4eb6-4a9c-8173-3ab267307e3a/'}
    return MockedResponse(data, 200)


@pytest.fixture
def returned_vendor_item2():
    data = {'url': 'https://www.neb.com', '@id': '/vendors/new-england-biolabs/', 'aliases': [], 'status': 'in review by lab', 'description': '', 'award': '/awards/1U01CA200059-01/', 'uuid': 'b31106bc-8535-4448-903e-854af460b21e', 'lab': '/labs/4dn-dcic-lab/', 'date_created': '2016-12-08T18:31:47.824418+00:00', '@type': ['Vendor', 'Item'], 'schema_version': '1', 'title': 'New England Biolabs', 'name': 'new-england-biolabs', 'submitted_by': '/users/986b362f-4eb6-4a9c-8173-3ab267307e3a/'}
    return MockedResponse(data, 200)


@pytest.fixture
def returned_vendor_item3():
    data = {'url': 'http://www.worthington-biochem.com', '@id': '/vendors/worthington-biochemical/', 'aliases': [], 'status': 'in review by lab', 'description': '', 'award': '/awards/1U01CA200059-01/', 'uuid': 'b31106bc-8535-4448-903e-854af460b21d', 'lab': '/labs/4dn-dcic-lab/', 'date_created': '2016-12-08T18:31:47.807726+00:00', '@type': ['Vendor', 'Item'], 'schema_version': '1', 'title': 'Worthington Biochemical', 'name': 'worthington-biochemical', 'submitted_by': '/users/986b362f-4eb6-4a9c-8173-3ab267307e3a/'}
    return MockedResponse(data, 200)


@pytest.fixture
def returned_post_new_vendor():
    data = {'status': 'success', '@type': ['result'], '@graph': [{'title': 'Test Vendor2', 'date_created': '2016-11-10T16:14:28.097832+00:00', 'submitted_by': '/users/986b362f-4eb6-4a9c-8173-3ab267307e3a/', 'aliases': ['dcic:vendor_test2'], 'name': 'test-vendor', 'status': 'in review by lab', 'uuid': 'ab487748-5904-42c8-9a8b-47f82df9f049', '@type': ['Vendor', 'Item'], 'schema_version': '1', 'url': 'http://www.test_vendor.com', '@id': '/vendors/test-vendor/', 'description': 'test description'}]}
    return MockedResponse(data, 201)


@pytest.fixture
def returned__patch_vendor():
    data = {'@type': ['result'], 'status': 'success', '@graph': [{'name': 'test-vendor', 'aliases': ['dcic:vendor_test'], 'schema_version': '1', 'description': 'test description new', 'status': 'in review by lab', 'title': 'Test Vendor', 'date_created': '2016-11-10T16:12:45.436813+00:00', 'url': 'http://www.test_vendor.com', '@id': '/vendors/test-vendor/', 'uuid': '004e2c5e-9825-43e2-98d2-fa078dd68be2', 'submitted_by': '/users/986b362f-4eb6-4a9c-8173-3ab267307e3a/', '@type': ['Vendor', 'Item']}]}
    return MockedResponse(data, 200)


@pytest.fixture
def award_dict():
    return {'properties': {'project': {'type': 'string', 'title': 'Project', 'description': 'The name of the consortium project', 'enum': ['4DN', 'External']}, 'start_date': {'anyOf': [{'format': 'date-time'}, {'format': 'date'}], 'type': 'string', 'title': 'Start date', 'comment': 'Date can be submitted as YYYY-MM-DD or YYYY-MM-DDTHH:MM:SSTZD (TZD is the time zone designator; use Z to express time in UTC or for time expressed in local time add a time zone offset from UTC +HH:MM or -HH:MM).'}, '@id': {'type': 'string', 'title': 'ID', 'calculatedProperty': True}, 'description': {'type': 'string', 'rdfs:subPropertyOf': 'dc:description', 'title': 'Description'}, 'end_date': {'anyOf': [{'format': 'date-time'}, {'format': 'date'}], 'type': 'string', 'title': 'End date', 'comment': 'Date can be submitted as YYYY-MM-DD or YYYY-MM-DDTHH:MM:SSTZD (TZD is the time zone designator; use Z to express time in UTC or for time expressed in local time add a time zone offset from UTC +HH:MM or -HH:MM).'}, 'name': {'uniqueKey': True, 'type': 'string', 'pattern': '^[A-Za-z0-9\\-]+$', 'title': 'Number', 'description': 'The official grant number from the NIH database, if applicable'}, '@type': {'items': {'type': 'string'}, 'type': 'array', 'title': 'Type', 'calculatedProperty': True}, 'submitted_by': {'serverDefault': 'userid', 'permission': 'import_items', 'type': 'string', 'rdfs:subPropertyOf': 'dc:creator', 'title': 'Submitted by', 'readonly': True, 'linkTo': 'User', 'comment': 'Do not submit, value is assigned by the server. The user that created the object.'}, 'date_created': {'serverDefault': 'now', 'permission': 'import_items', 'type': 'string', 'rdfs:subPropertyOf': 'dc:created', 'title': 'Date created', 'readonly': True, 'comment': 'Do not submit, value is assigned by the server. The date the object is created.', 'anyOf': [{'format': 'date-time'}, {'format': 'date'}]}, 'title': {'type': 'string', 'rdfs:subPropertyOf': 'dc:title', 'title': 'Name', 'description': 'The grant name from the NIH database, if applicable.'}, 'viewing_group': {'type': 'string', 'title': 'View access group', 'description': 'The group that determines which set of data the user has permission to view.', 'enum': ['4DN', 'Not 4DN']}, 'schema_version': {'type': 'string', 'pattern': '^\\d+(\\.\\d+)*$', 'title': 'Schema Version', 'default': '1', 'requestMethod': [], 'comment': 'Do not submit, value is assigned by the server. The version of the JSON schema that the server uses to validate the object. Schema version indicates generation of schema used to save version to to enable upgrade steps to work. Individual schemas should set the default.'}, 'url': {'type': 'string', 'rdfs:subPropertyOf': 'rdfs:seeAlso', 'format': 'uri', 'title': 'URL', 'description': 'An external resource with additional information about the grant.', '@type': '@id'}, 'uuid': {'serverDefault': 'uuid4', 'permission': 'import_items', 'type': 'string', 'format': 'uuid', 'title': 'UUID', 'readonly': True, 'requestMethod': 'POST'}, 'status': {'enum': ['released', 'current', 'revoked', 'deleted', 'replaced', 'in review by lab', 'in review by project', 'released to project'], 'permission': 'import_items', 'type': 'string', 'title': 'Status', 'readonly': True, 'default': 'released'}, 'pi': {'linkTo': 'User', 'type': 'string', 'title': 'P.I.', 'description': 'Principle Investigator of the grant.', 'comment': 'See user.json for available identifiers.'}}, 'type': 'object', 'mixinProperties': [{'$ref': 'mixins.json#/schema_version'}, {'$ref': 'mixins.json#/uuid'}, {'$ref': 'mixins.json#/submitted'}, {'$ref': 'mixins.json#/status'}], 'title': 'Grant', 'required': ['name'], 'boost_values': {'pi.title': 1.0, 'title': 1.0, 'name': 1.0}, 'identifyingProperties': ['uuid', 'name', 'title'], 'additionalProperties': False, '$schema': 'http://json-schema.org/draft-04/schema#', '@type': ['JSONSchema'], 'id': '/profiles/award.json'}


@pytest.fixture
def vendor_raw_xls_fields():
    return[
           ['#Field Name:', 'description', 'submitted_by', 'award', 'lab', 'aliases', 'url', 'notes', '*title', 'name', 'date_created', 'uuid', 'status', 'schema_version'],
           ['#Field Type:', 'string', 'Item:User', 'Item:Award', 'Item:Lab', 'array of strings', 'string', 'string', 'string', 'string', 'string', 'string', 'string', 'string'],
           ['#Description:', 'A plain text description of the source.', '', 'Grant associated with the submission.', 'Lab associated with the submission.', 'Lab specific identifiers to reference an object.', 'An external resource with additional information about the source.', 'DCIC internal notes.', 'The complete name of the originating lab or vendor. ', '', '', '', '', ''],
           ['#Additional Info:', '', 'Do not submit, value is assigned by the server. The user that created the object.', 'See award.json for list of available identifiers.', 'See lab.json for list of available identifiers.', '', '', '', '', 'Do not submit, value is auto generated from the title as lower cased and hyphen delimited.', 'Do not submit, value is assigned by the server. The date the object is created.', '', "Choices:['released', 'current', 'revoked', 'deleted', 'replaced', 'in review by lab', 'in review by project', 'released to project']", 'Do not submit, value is assigned by the server. The version of the JSON schema that the server uses to validate the object. Schema version indicates generation of schema used to save version to to enable upgrade steps to work. Individual schemas should set the default.']
          ]


@pytest.fixture
def returned_vendor_existing_item():
    data = {'title': 'Test Vendor2', 'date_created': '2016-11-10T16:14:28.097832+00:00', 'submitted_by': '/users/986b362f-4eb6-4a9c-8173-3ab267307e3a/', 'aliases': ['dcic:vendor_test2'], 'name': 'test-vendor', 'status': 'in review by lab', 'uuid': 'ab487748-5904-42c8-9a8b-47f82df9f049', '@type': ['Vendor', 'Item'], 'schema_version': '1', 'url': 'http://www.test_vendor.com', '@id': '/vendors/test-vendor/', 'description': 'test description'}
    return MockedResponse(data, 200)


@pytest.fixture
def returned_user_me_submit_for_no_lab():
    data = {
        'first_name': 'Bin', 'last_name': 'Li', 'email': 'bil022@ucsd.edu', 'viewing_groups': ['4DN'],
        'title': 'Bin Li', 'display_title': 'Bin Li', 'uuid': 'da4f53e5-4e54-4ae7-ad75-ba47316a8bfa',
        '@id': '/users/da4f53e5-4e54-4ae7-ad75-ba47316a8bfa/', '@type': ['User', 'Item'],
        'link_id': '~users~da4f53e5-4e54-4ae7-ad75-ba47316a8bfa~', 'status': 'current'
    }
    return MockedResponse(data, 307)


@pytest.fixture
def returned_user_me_submit_for_one_lab():
    data = {
        'first_name': 'Bin', 'last_name': 'Li', 'email': 'bil022@ucsd.edu', 'viewing_groups': ['4DN'],
        'title': 'Bin Li', 'display_title': 'Bin Li', 'uuid': 'da4f53e5-4e54-4ae7-ad75-ba47316a8bfa',
        '@id': '/users/da4f53e5-4e54-4ae7-ad75-ba47316a8bfa/', '@type': ['User', 'Item'],
        'link_id': '~users~da4f53e5-4e54-4ae7-ad75-ba47316a8bfa~', 'status': 'current',
        'submits_for': [
            {'uuid': '795847de-20b6-4f8c-ba8d-185215469cbf', 'display_title': 'Bing Ren, UCSD', 'link_id': '~labs~bing-ren-lab~'}
        ]
    }
    return MockedResponse(data, 307)


@pytest.fixture
def returned_user_me_submit_for_two_labs():
    data = {
        'first_name': 'Bin', 'last_name': 'Li', 'email': 'bil022@ucsd.edu', 'viewing_groups': ['4DN'],
        'title': 'Bin Li', 'display_title': 'Bin Li', 'uuid': 'da4f53e5-4e54-4ae7-ad75-ba47316a8bfa',
        '@id': '/users/da4f53e5-4e54-4ae7-ad75-ba47316a8bfa/', '@type': ['User', 'Item'],
        'link_id': '~users~da4f53e5-4e54-4ae7-ad75-ba47316a8bfa~', 'status': 'current',
        'submits_for': [
            {'uuid': '795847de-20b6-4f8c-ba8d-185215469cbf', 'display_title': 'Bing Ren, UCSD', 'link_id': '~labs~bing-ren-lab~'},
            {'uuid': '895847de-20b6-4f8c-ba8d-185215469cbf', 'display_title': 'Ben Ring, USDC', 'link_id': '~labs~ben-ring-lab~'}
        ]
    }
    return MockedResponse(data, 307)


@pytest.fixture
def returned_lab_w_one_award():
    data = {
        'awards': [
            {
                'viewing_group': '4DN', 'title': 'SAN DIEGO CENTER FOR 4D NUCLEOME RESEARCH',
                'link_id': '~awards~1U54DK107977-01~', '@id': '/awards/1U54DK107977-01/',
                'pi': {'link_id': '~users~e3159ffc-a5a9-43a1-8cfa-90b776c39788~', 'uuid': 'e3159ffc-a5a9-43a1-8cfa-90b776c39788', 'display_title': 'Bing Ren'},
                'status': 'current', 'uuid': '4871e338-b07d-4665-a00a-357648e5bad6', 'display_title': 'SAN DIEGO CENTER FOR 4D NUCLEOME RESEARCH',
                'name': '1U54DK107977-01', '@type': ['Award', 'Item'], 'project': '4DN'
            }
        ],
        'title': 'Bing Ren, UCSD', 'link_id': '~labs~bing-ren-lab~', 'institute_label': 'UCSD', '@id': '/labs/bing-ren-lab/',
        'pi': {'link_id': '~users~e3159ffc-a5a9-43a1-8cfa-90b776c39788~', 'uuid': 'e3159ffc-a5a9-43a1-8cfa-90b776c39788', 'display_title': 'Bing Ren'},
        'status': 'current', 'uuid': '795847de-20b6-4f8c-ba8d-185215469cbf', 'display_title': 'Bing Ren, UCSD',
        'name': 'bing-ren-lab', '@type': ['Lab', 'Item']
    }
    return MockedResponse(data, 200)


@pytest.fixture
def returned_otherlab_w_one_award():
    data = {
        'awards': [
            {
                'viewing_group': 'Not 4DN', 'title': 'THE SAN DIEGO EPIGENOME CENTER',
                'link_id': '~awards~1U01ES017166-01~', '@id': '/awards/1U01ES017166-01/',
                'pi': {'link_id': '~users~e3159ffc-a5a9-43a1-8cfa-90b776c39788~', 'uuid': 'e3159ffc-a5a9-43a1-8cfa-90b776c39788', 'display_title': 'Bing Ren'},
                'status': 'current', 'uuid': '1dbecc95-ec91-4081-a862-c79d18a8d0bd', 'display_title': 'THE SAN DIEGO EPIGENOME CENTER',
                'name': '1U01ES017166-01', '@type': ['Award', 'Item'], 'project': 'External'
            }
        ],
        'uuid': '895847de-20b6-4f8c-ba8d-185215469cbf', 'display_title': 'Ben Ring, USDC', 'link_id': '~labs~ben-ring-lab~',
        'title': 'Ben Ring, USDC', '@id': '/labs/ben-ring-lab/',
        'status': 'current', 'name': 'ben-ring-lab', '@type': ['Lab', 'Item']
    }
    return MockedResponse(data, 200)


@pytest.fixture
def returned_lab_w_two_awards():
    data = {
        'awards': [
            {
                'viewing_group': '4DN', 'title': 'SAN DIEGO CENTER FOR 4D NUCLEOME RESEARCH',
                'link_id': '~awards~1U54DK107977-01~', '@id': '/awards/1U54DK107977-01/',
                'pi': {'link_id': '~users~e3159ffc-a5a9-43a1-8cfa-90b776c39788~', 'uuid': 'e3159ffc-a5a9-43a1-8cfa-90b776c39788', 'display_title': 'Bing Ren'},
                'status': 'current', 'uuid': '4871e338-b07d-4665-a00a-357648e5bad6', 'display_title': 'SAN DIEGO CENTER FOR 4D NUCLEOME RESEARCH',
                'name': '1U54DK107977-01', '@type': ['Award', 'Item'], 'project': '4DN'
            },
            {
                'viewing_group': 'Not 4DN', 'title': 'THE SAN DIEGO EPIGENOME CENTER',
                'link_id': '~awards~1U01ES017166-01~', '@id': '/awards/1U01ES017166-01/',
                'pi': {'link_id': '~users~e3159ffc-a5a9-43a1-8cfa-90b776c39788~', 'uuid': 'e3159ffc-a5a9-43a1-8cfa-90b776c39788', 'display_title': 'Bing Ren'},
                'status': 'current', 'uuid': '1dbecc95-ec91-4081-a862-c79d18a8d0bd', 'display_title': 'THE SAN DIEGO EPIGENOME CENTER',
                'name': '1U01ES017166-01', '@type': ['Award', 'Item'], 'project': 'External'
            }
        ],
        'title': 'Bing Ren, UCSD', 'link_id': '~labs~bing-ren-lab~', 'institute_label': 'UCSD', '@id': '/labs/bing-ren-lab/',
        'pi': {'link_id': '~users~e3159ffc-a5a9-43a1-8cfa-90b776c39788~', 'uuid': 'e3159ffc-a5a9-43a1-8cfa-90b776c39788', 'display_title': 'Bing Ren'},
        'status': 'current', 'uuid': '795847de-20b6-4f8c-ba8d-185215469cbf', 'display_title': 'Bing Ren, UCSD',
        'name': 'bing-ren-lab', '@type': ['Lab', 'Item']
    }
    return MockedResponse(data, 200)


@pytest.fixture
def returned_otherlab_w_two_awards():
    data = {
        'awards': [
            {
                'viewing_group': 'Not 4DN', 'title': 'THE SAN DIEGO EPIGENOME CENTER',
                'link_id': '~awards~1U01ES017166-01~', '@id': '/awards/1U01ES017166-01/',
                'pi': {'link_id': '~users~e3159ffc-a5a9-43a1-8cfa-90b776c39788~', 'uuid': 'e3159ffc-a5a9-43a1-8cfa-90b776c39788', 'display_title': 'Bing Ren'},
                'status': 'current', 'uuid': '1dbecc95-ec91-4081-a862-c79d18a8d0bd', 'display_title': 'THE SAN DIEGO EPIGENOME CENTER',
                'name': '1U01ES017166-01', '@type': ['Award', 'Item'], 'project': 'External'
            },
            {
                'viewing_group': 'Not 4DN', 'title': 'THE OTHER AWARD',
                'link_id': '~awards~7777777~', '@id': '/awards/7777777/',
                'pi': {'link_id': '~users~e3159ffc-a5a9-43a1-8cfa-90b776c39788~', 'uuid': 'e3159ffc-a5a9-43a1-8cfa-90b776c39788', 'display_title': 'Bing Ren'},
                'status': 'current', 'uuid': '2dbecc95-ec91-4081-a862-c79d18a8d0bd', 'display_title': 'THE OTHER AWARD',
                'name': '7777777', '@type': ['Award', 'Item'], 'project': 'External'
            }
        ],
        'uuid': '895847de-20b6-4f8c-ba8d-185215469cbf', 'display_title': 'Ben Ring, USDC', 'link_id': '~labs~ben-ring-lab~',
        'title': 'Ben Ring, USDC', '@id': '/labs/ben-ring-lab/',
        'status': 'current', 'name': 'ben-ring-lab', '@type': ['Lab', 'Item']
    }
    return MockedResponse(data, 200)
