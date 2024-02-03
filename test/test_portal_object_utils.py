from dcicutils.portal_object_utils import PortalObject
from dcicutils.portal_utils import Portal
from unittest import mock

TEST_OBJECT_RAW_JSON = {
    "status": "in review",
    "consortia": [
        "358aed10-9b9d-4e26-ab84-4bd162da182b"
    ],
    "parameters": {
        "autoadd": "{\"submission_centers\": [\"9626d82e-8110-4213-ac75-0a50adf890ff\"]}",
        "datafile": "test_submission_from_doug_20231106.xlsx",
        "post_only": "False",
        "consortium": "/consortia/358aed10-9b9d-4e26-ab84-4bd162da182b/",
        "patch_only": "False",
        "sheet_utils": "False",
        "validate_only": "False",
        "ingestion_type": "metadata_bundle",
        "submission_center": "/submission-centers/9626d82e-8110-4213-ac75-0a50adf890ff/",
        "ingestion_directory": "/Users/dmichaels/repos/cgap/submitr/testdata/demo"
    },
    "object_name": "ef7725d2-7c0b-4fa5-b20f-4ca52df7499d/datafile.xlsx",
    "date_created": "2024-02-02T19:13:20.958271+00:00",
    "submitted_by": "74fef71a-dfc1-4aa4-acc0-cedcb7ac1d68",
    "submission_id": "ef7725d2-7c0b-4fa5-b20f-4ca52df7499d",
    "ingestion_type": "metadata_bundle",
    "schema_version": "1",
    "additional_data": {
        "upload_info": [
            {
                "uuid": "6dbe0669-1eea-402d-beb9-e5388ba1e584",
                "filename": "first_file.fastq"
            },
            {
                "uuid": "f5ac5d98-1f85-44f4-8bad-b4488fbdda7e",
                "filename": "second_file.fastq"
            }
        ],
        "validation_output": [
            "Submission UUID: ef7725d2-7c0b-4fa5-b20f-4ca52df7499d",
            "Status: OK",
            "File: test_submission_from_doug_20231106.xlsx",
            "S3 File: s3://smaht-unit-testing-metadata-bundles/ef7725d2-7c0b-4fa5-b20f-4ca52df7499d/datafile.xlsx",
            "Details: s3://smaht-unit-testing-metadata-bundles/ef7725d2-7c0b-4fa5-b20f-4ca52df7499d/submission.json",
            "Total: 7",
            "Types: 4",
            "Created: 0",
            "Updated: 7",
            "Skipped: 0",
            "Checked: 0"
        ]
    },
    "processing_status": {
        "state": "done",
        "outcome": "success",
        "progress": "complete"
    },
    "submission_centers": [
        "9626d82e-8110-4213-ac75-0a50adf890ff"
    ],
    "uuid": "ef7725d2-7c0b-4fa5-b20f-4ca52df7499d"
}

TEST_OBJECT_DATABASE_JSON = {
    "status": "in review",
    "consortia": [
        {
            "status": "released",
            "display_title": "SMaHT Consortium",
            "@type": [
                "Consortium",
                "Item"
            ],
            "uuid": "358aed10-9b9d-4e26-ab84-4bd162da182b",
            "@id": "/consortia/358aed10-9b9d-4e26-ab84-4bd162da182b/",
            "principals_allowed": {
                "view": [
                    "group.admin",
                    "group.read-only-admin",
                    "remoteuser.EMBED",
                    "remoteuser.INDEXER"
                ],
                "edit": [
                    "group.admin"
                ]
            }
        }
    ],
    "parameters": {
        "autoadd": "{\"submission_centers\": [\"9626d82e-8110-4213-ac75-0a50adf890ff\"]}",
        "datafile": "test_submission_from_doug_20231106.xlsx",
        "post_only": "False",
        "consortium": "/consortia/358aed10-9b9d-4e26-ab84-4bd162da182b/",
        "patch_only": "False",
        "sheet_utils": "False",
        "validate_only": "False",
        "ingestion_type": "metadata_bundle",
        "submission_center": "/submission-centers/9626d82e-8110-4213-ac75-0a50adf890ff/",
        "ingestion_directory": "/Users/dmichaels/repos/cgap/submitr/testdata/demo"
    },
    "object_name": "ef7725d2-7c0b-4fa5-b20f-4ca52df7499d/datafile.xlsx",
    "date_created": "2024-02-02T19:13:20.958271+00:00",
    "submitted_by": {
        "display_title": "David Michaels",
        "@type": [
            "User",
            "Item"
        ],
        "status": "current",
        "@id": "/users/74fef71a-dfc1-4aa4-acc0-cedcb7ac1d68/",
        "uuid": "74fef71a-dfc1-4aa4-acc0-cedcb7ac1d68",
        "principals_allowed": {
            "view": [
                "group.admin",
                "group.read-only-admin",
                "remoteuser.EMBED",
                "remoteuser.INDEXER"
            ],
            "edit": [
                "group.admin"
            ]
        }
    },
    "submission_id": "ef7725d2-7c0b-4fa5-b20f-4ca52df7499d",
    "ingestion_type": "metadata_bundle",
    "schema_version": "1",
    "additional_data": {
        "upload_info": [
            {
                "uuid": "6dbe0669-1eea-402d-beb9-e5388ba1e584",
                "filename": "first_file.fastq"
            },
            {
                "uuid": "f5ac5d98-1f85-44f4-8bad-b4488fbdda7e",
                "filename": "second_file.fastq"
            }
        ],
        "validation_output": [
            "Submission UUID: ef7725d2-7c0b-4fa5-b20f-4ca52df7499d",
            "Status: OK",
            "File: test_submission_from_doug_20231106.xlsx",
            "S3 File: s3://smaht-unit-testing-metadata-bundles/ef7725d2-7c0b-4fa5-b20f-4ca52df7499d/datafile.xlsx",
            "Details: s3://smaht-unit-testing-metadata-bundles/ef7725d2-7c0b-4fa5-b20f-4ca52df7499d/submission.json",
            "Total: 7",
            "Types: 4",
            "Created: 0",
            "Updated: 7",
            "Skipped: 0",
            "Checked: 0"
        ]
    },
    "processing_status": {
        "state": "done",
        "outcome": "success",
        "progress": "complete"
    },
    "submission_centers": [
        {
            "@type": [
                "SubmissionCenter",
                "Item"
            ],
            "status": "public",
            "uuid": "9626d82e-8110-4213-ac75-0a50adf890ff",
            "@id": "/submission-centers/9626d82e-8110-4213-ac75-0a50adf890ff/",
            "display_title": "SMaHT DAC",
            "principals_allowed": {
                "view": [
                    "group.admin",
                    "group.read-only-admin",
                    "remoteuser.EMBED",
                    "remoteuser.INDEXER"
                ],
                "edit": [
                    "group.admin"
                ]
            }
        }
    ],
    "@id": "/ingestion-submissions/ef7725d2-7c0b-4fa5-b20f-4ca52df7499d/",
    "@type": [
        "IngestionSubmission",
        "Item"
    ],
    "uuid": "ef7725d2-7c0b-4fa5-b20f-4ca52df7499d",
    "principals_allowed": {
        "view": [
            "group.admin",
            "group.read-only-admin",
            "remoteuser.EMBED",
            "remoteuser.INDEXER"
        ],
        "edit": [
            "group.admin"
        ]
    },
    "display_title": "IngestionSubmission from 2024-02-02",
    "@context": "/terms/",
    "actions": [
        {
            "name": "create",
            "title": "Create",
            "profile": "/profiles/IngestionSubmission.json",
            "href": "/ingestion-submissions/ef7725d2-7c0b-4fa5-b20f-4ca52df7499d/?currentAction=create"
        },
        {
            "name": "edit",
            "title": "Edit",
            "profile": "/profiles/IngestionSubmission.json",
            "href": "/ingestion-submissions/ef7725d2-7c0b-4fa5-b20f-4ca52df7499d/?currentAction=edit"
        }
    ],
    "aggregated-items": {},
    "validation-errors": [],
    "aliases": ["foo", "bar"]
}

TEST_OBJECT_SCHEMA_JSON = {
    "title": "Ingestion Submission",
    "description": "Schema for metadata related to submitted ingestion requests",
    "$id": "/profiles/ingestion_submission.json",
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "required": [
        "ingestion_type"
    ],
    "anyOf": [
        {
            "required": [
                "submission_centers"
            ]
        },
        {
            "required": [
                "consortia"
            ]
        }
    ],
    "identifyingProperties": [
        "aliases",
        "uuid"
    ],
    "additionalProperties": False,
    "mixinProperties": [
        {
            "$ref": "mixins.json#/aliases"
        },
        {
            "$ref": "mixins.json#/attribution"
        },
        {
            "$ref": "mixins.json#/documents"
        },
        {
            "$ref": "mixins.json#/modified"
        },
        {
            "$ref": "mixins.json#/schema_version"
        },
        {
            "$ref": "mixins.json#/status"
        },
        {
            "$ref": "mixins.json#/submitted"
        },
        {
            "$ref": "mixins.json#/uuid"
        }
    ],
    "properties": {
        "uuid": {
            "title": "UUID",
            "type": "string",
            "format": "uuid",
            "exclude_from": [
                "FFedit-create"
            ],
            "serverDefault": "uuid4",
            "permission": "restricted_fields",
            "requestMethod": "POST",
            "readonly": True
        },
        "date_created": {
            "rdfs:subPropertyOf": "dc:created",
            "title": "Date Created",
            "exclude_from": [
                "FFedit-create"
            ],
            "type": "string",
            "anyOf": [
                {
                    "format": "date-time"
                },
                {
                    "format": "date"
                }
            ],
            "serverDefault": "now",
            "permission": "restricted_fields",
            "readonly": True
        },
        "submitted_by": {
            "rdfs:subPropertyOf": "dc:creator",
            "title": "Submitted By",
            "exclude_from": [
                "FFedit-create"
            ],
            "type": "string",
            "linkTo": "User",
            "serverDefault": "userid",
            "permission": "restricted_fields",
            "readonly": True
        },
        "status": {
            "title": "Status",
            "type": "string",
            "default": "in review",
            "permission": "restricted_fields",
            "enum": [
                "public",
                "draft",
                "released",
                "in review",
                "obsolete",
                "deleted"
            ],
            "readonly": True
        },
        "schema_version": {
            "title": "Schema Version",
            "internal_comment": "Do not submit, value is assigned by the server ...",
            "type": "string",
            "exclude_from": [
                "FFedit-create"
            ],
            "pattern": "^\\d+(\\.\\d+)*$",
            "requestMethod": [],
            "default": "1"
        },
        "last_modified": {
            "title": "Last Modified",
            "exclude_from": [
                "FFedit-create"
            ],
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "date_modified": {
                    "title": "Date Modified",
                    "description": "Do not submit, value is assigned by the server. The date the object is modified.",
                    "type": "string",
                    "anyOf": [
                        {
                            "format": "date-time"
                        },
                        {
                            "format": "date"
                        }
                    ],
                    "permission": "restricted_fields"
                },
                "modified_by": {
                    "title": "Modified By",
                    "description": "Do not submit, value is assigned by the server. The user that modfied the object.",
                    "type": "string",
                    "linkTo": "User",
                    "permission": "restricted_fields"
                }
            }
        },
        "documents": {
            "title": "Documents",
            "description": "Documents that provide additional information (not data file).",
            "comment": "See Documents sheet or collection for existing items.",
            "type": "array",
            "uniqueItems": True,
            "items": {
                "title": "Document",
                "description": "A document that provides additional information (not data file).",
                "type": "string",
                "linkTo": "Document"
            }
        },
        "submission_centers": {
            "title": "Submission Centers",
            "description": "Submission Centers associated with this item.",
            "type": "array",
            "uniqueItems": True,
            "items": {
                "type": "string",
                "linkTo": "SubmissionCenter"
            },
            "serverDefault": "user_submission_centers"
        },
        "consortia": {
            "title": "Consortia",
            "description": "Consortia associated with this item.",
            "type": "array",
            "uniqueItems": True,
            "items": {
                "type": "string",
                "linkTo": "Consortium"
            },
            "permission": "restricted_fields",
            "readonly": True
        },
        "aliases": {
            "title": "Aliases",
            "description": "Institution-specific ID (e.g. bgm:cohort-1234-a).",
            "type": "array",
            "comment": "Colon separated lab name and lab identifier, no slash. (e.g. dcic-lab:42).",
            "uniqueItems": True,
            "ff_flag": "clear clone",
            "permission": "restricted_fields",
            "items": {
                "uniqueKey": "alias",
                "title": "ID Alias",
                "description": "Institution-specific ID (e.g. bgm:cohort-1234-a).",
                "type": "string",
                "pattern": "^[^\\s\\\\\\/]+:[^\\s\\\\\\/]+$"
            },
            "readonly": True
        },
        "additional_data": {
            "title": "Additional Data",
            "description": "Additional structured information resulting from processing ...",
            "type": "object",
            "additionalProperties": True
        },
        "errors": {
            "title": "Errors",
            "description": "A list of error messages if processing was aborted before results were obtained.",
            "type": "array",
            "minItems": 1,
            "uniqueItems": True,
            "items": {
                "title": "Error Message",
                "description": "One of possibly several reasons that processing was not completed.",
                "type": "string"
            }
        },
        "ingestion_type": {
            "title": "Ingestion Type",
            "description": "The type of processing requested for this submission.",
            "type": "string",
            "enum": [
                "accessioning",
                "data_bundle",
                "metadata_bundle",
                "simulated_bundle"
            ]
        },
        "object_bucket": {
            "title": "Object Bucket",
            "description": "The name of the S3 bucket in which the 'object_name' resides.",
            "type": "string"
        },
        "object_name": {
            "title": "Object Name",
            "description": "The name of the S3 object corresponding to the submitted document.",
            "type": "string"
        },
        "parameters": {
            "title": "Parameters",
            "description": "A record of explicitly offered form parameters in the submission request.",
            "type": "object",
            "additionalProperties": True
        },
        "processing_status": {
            "title": "Processing Status",
            "description": "A structured description of what has happened so far as the submission is processed.",
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "state": {
                    "title": "State",
                    "description": "A state machine description of how processing ...",
                    "type": "string",
                    "enum": [
                        "created",
                        "submitted",
                        "processing",
                        "done"
                    ],
                    "default": "created"
                },
                "outcome": {
                    "title": "Outcome",
                    "description": "A token describing the nature of the final outcome...",
                    "type": "string",
                    "enum": [
                        "unknown",
                        "success",
                        "failure",
                        "error"
                    ],
                    "default": "unknown"
                },
                "progress": {
                    "title": "Progress",
                    "description": "An adjectival word or phrase assessing progress ...",
                    "type": "string",
                    "default": "unavailable"
                }
            }
        },
        "result": {
            "title": "Result",
            "description": "An object representing a result if processing ...",
            "type": "object",
            "additionalProperties": True
        },
        "submission_id": {
            "title": "Submission ID",
            "description": "The name of a folder in the S3 bucket that contains ...",
            "type": "string"
        },
        "@id": {
            "title": "ID",
            "type": "string",
            "calculatedProperty": True
        },
        "@type": {
            "title": "Type",
            "type": "array",
            "items": {
                "type": "string"
            },
            "calculatedProperty": True
        },
        "principals_allowed": {
            "title": "principals_allowed",
            "description": "Calculated permissions used for ES filtering",
            "type": "object",
            "properties": {
                "view": {
                    "type": "string"
                },
                "edit": {
                    "type": "string"
                }
            },
            "calculatedProperty": True
        },
        "display_title": {
            "title": "Display Title",
            "description": "A calculated title for every object",
            "type": "string",
            "calculatedProperty": True
        }
    },
    "@type": [
        "JSONSchema"
    ],
    "rdfs:seeAlso": "/terms/IngestionSubmission",
    "children": [],
    "rdfs:subClassOf": "/profiles/Item.json",
    "isAbstract": False
}

TEST_OBJECT_UUID = TEST_OBJECT_RAW_JSON["uuid"]
TEST_OBJECT_IDENTIFYING_PATH = f"/{TEST_OBJECT_DATABASE_JSON['@type'][0]}/{TEST_OBJECT_UUID}"
TEST_OBJECT_IDENTIFYING_PATHS = [TEST_OBJECT_IDENTIFYING_PATH, f"/{TEST_OBJECT_UUID}"]
TEST_OBJECT_TYPE = "IngestionSubmission"
TEST_OBJECT_TYPES = ["IngestionSubmission", "Item"]


class MockPortal(Portal):
    def __init__(self, arg=None, env=None, server=None, app=None, raise_exception=True):
        pass
    class _Response:
        def __init__(self, data, status_code):
            self._data = data
            self._status_code = status_code
        def json(self):  # noqa
            return self._data
        @property  # noqa
        def status_code(self):
            return self._status_code
    def get(self, url, follow=True, raw=False, database=False, raise_for_status=False, **kwargs):  # noqa
        return MockPortal._Response(TEST_OBJECT_DATABASE_JSON, 200)
    def get_schema(self, schema_name):  # noqa
        return TEST_OBJECT_SCHEMA_JSON


def test_compare():

    portal_object = PortalObject(None, TEST_OBJECT_RAW_JSON)
    assert portal_object.data == TEST_OBJECT_RAW_JSON
    assert not portal_object.portal
    assert portal_object.uuid == TEST_OBJECT_UUID
    assert not portal_object.types
    assert not portal_object.type
    assert not portal_object.schema
    assert not portal_object.identifying_properties
    assert not portal_object.identifying_paths
    assert not portal_object.identifying_path
    assert portal_object.compare(TEST_OBJECT_RAW_JSON) == {}

    portal_object = PortalObject(None, TEST_OBJECT_DATABASE_JSON)
    assert portal_object.data == TEST_OBJECT_DATABASE_JSON
    assert not portal_object.portal
    assert portal_object.uuid == TEST_OBJECT_UUID
    assert portal_object.types == TEST_OBJECT_TYPES
    assert portal_object.type == TEST_OBJECT_TYPE
    assert not portal_object.schema
    assert not portal_object.identifying_properties
    assert portal_object.identifying_paths == TEST_OBJECT_IDENTIFYING_PATHS
    assert portal_object.identifying_path == TEST_OBJECT_IDENTIFYING_PATH
    assert portal_object.compare(TEST_OBJECT_DATABASE_JSON) == {}

    portal_object_copy = portal_object.copy()
    assert portal_object.data == portal_object_copy.data
    assert portal_object.portal == portal_object_copy.portal

    with mock.patch("dcicutils.portal_utils.Portal", MockPortal()) as portal:

        return  # xyzzy
        portal_object = PortalObject(portal, TEST_OBJECT_RAW_JSON)
        assert portal_object.data == TEST_OBJECT_RAW_JSON
        assert portal_object.portal == portal
        assert portal_object.uuid == TEST_OBJECT_UUID
        assert not portal_object.type
        assert not portal_object.types
        assert portal_object.schema == TEST_OBJECT_SCHEMA_JSON
        assert portal_object.identifying_properties == ["uuid"]

        portal_object_found = portal_object.lookup()
        assert portal_object_found.uuid == portal_object.uuid
        assert portal_object_found.types == ["IngestionSubmission", "Item"]
        assert portal_object_found.type == "IngestionSubmission"
        assert portal_object_found.identifying_properties == ["uuid", "aliases"]
        assert portal_object_found.identifying_path == TEST_OBJECT_IDENTIFYING_PATH
        assert portal_object_found.identifying_paths == [*TEST_OBJECT_IDENTIFYING_PATHS,
                                                         "/IngestionSubmission/foo", "/foo",
                                                         "/IngestionSubmission/bar", "/bar"]
