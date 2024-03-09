from contextlib import contextmanager
import inspect
import json
import os
import pytest
import re
from typing import Callable, List, Optional, Tuple, Union
from unittest import mock
from webtest import TestApp
from dcicutils.misc_utils import VirtualApp
from dcicutils.tmpfile_utils import temporary_file
from dcicutils.validation_utils import SchemaManager  # noqa
from dcicutils.structured_data import Portal, Schema, StructuredDataSet, _StructuredRowTemplate  # noqa

portal = Portal.create_for_testing([])
testapp = portal.vapp

THIS_TEST_MODULE_DIRECTORY = os.path.dirname(os.path.abspath(__file__))
TEST_FILES_DIR = f"{THIS_TEST_MODULE_DIRECTORY}/data_files/structured_data"
SAME_AS_EXPECTED_REFS = {}
SAME_AS_NOREFS = {}

# Same as in smaht-portal/.../project/loadxl
ITEM_INDEX_ORDER = [
        'access_key',
        'user',
        'consortium',
        'submission_center',
        'file_format',
        'quality_metric',
        'output_file',
        'reference_file',
        'reference_genome',
        'software',
        'tracking_item',
        'workflow',
        'workflow_run',
        'meta_workflow',
        'meta_workflow_run',
        'image',
        'document',
        'static_section',
        'page',
        'filter_set',
        'higlass_view_config',
        'ingestion_submission',
        'ontology_term',
        'protocol',
        'donor',
        'demographic',
        'medical_history',
        'diagnosis',
        'exposure',
        'therapeutic',
        'molecular_test',
        'death_circumstances',
        'tissue_collection',
        'tissue',
        'histology',
        'cell_line',
        'cell_culture',
        'cell_culture_mixture',
        'preparation_kit',
        'treatment',
        'sample_preparation',
        'tissue_sample',
        'cell_culture_sample',
        'cell_sample',
        'analyte',
        'analyte_preparation',
        'library',
        'library_preparation',
        'assay',
        'sequencer',
        'sequencing',
        'file_set',
        'unaligned_reads',
        'aligned_reads',
        'variant_calls',
]

# Ideally this flag would be False, i.e. we would like to use the actual/real (live)
# schemas that are defined in (the schemas directory of) this repo for testing; but at
# least currently (2024-01-10) these are undergoing a lot of change and leading to frequent
# and annoying test breakage; so setting this to True will cause these tests to use a dump of
# the schemas which were previously saved into a static file (data/test-files/schemas_dump.json).
# Note: to dump all schemes into single file -> show-schema all
USE_SAVED_SCHEMAS_RATHER_THAN_LIVE_SCHEMAS = True

if USE_SAVED_SCHEMAS_RATHER_THAN_LIVE_SCHEMAS:
    from functools import lru_cache
    from dcicutils.portal_utils import Portal as PortalBase
    @lru_cache(maxsize=1)  # noqa
    def _mocked_portal_get_schemas(self):
        with open(os.path.join(TEST_FILES_DIR, "schemas_dump.json"), "r") as f:
            schemas = json.load(f)
        return schemas
    PortalBase.get_schemas = _mocked_portal_get_schemas


def _load_json_from_file(file: str) -> dict:
    with open(os.path.join(TEST_FILES_DIR, file)) as f:
        return json.load(f)


def _pytest_kwargs(kwargs: List[dict]) -> List[dict]:
    # If any of the parameterized tests are marked as debug=True then only execute those.
    debug_kwargs = [kwarg for kwarg in kwargs if (kwarg.get("debug") is True)]
    return debug_kwargs or kwargs


@pytest.mark.parametrize("kwargs", _pytest_kwargs([  # test_parse_structured_data_parameterized
    {
        "rows": [
            r"uuid,status,principals.view,principals.edit,extensions#,data",
            r"some-uuid-a,public,pav-a,pae-a,alfa|bravo|charlie,123.4",
            r"some-uuid-b,public,pav-b,pae-b,delta|echo|foxtrot|golf,xyzzy"
        ],
        "as_file_name": "some_test.csv",
        "noschemas": True,
        "expected": {
            "SomeTest": [
                {
                    "uuid": "some-uuid-a",
                    "status": "public",
                    "principals": {"view": "pav-a", "edit": "pae-a"},
                    "extensions": ["alfa", "bravo", "charlie"],
                    "data": "123.4"
                },
                {
                    "uuid": "some-uuid-b",
                    "status": "public",
                    "principals": {"view": "pav-b", "edit": "pae-b"},
                    "extensions": ["delta", "echo", "foxtrot", "golf"],
                    "data": "xyzzy"
                }
            ]
        }
    },
    # ----------------------------------------------------------------------------------------------
    {
        "rows": [
            r"uuid,status,principals.view,principals.edit,extensions#,num,i,arr",
            r"some-uuid-a,public,pav-a,pae-a,alfa|bravo|charlie,123.4,617,hotel",
            r"some-uuid-b,public,pav-b,pae-b,delta|echo|foxtrot|golf,987,781,indigo\|juliet|kilo"
        ],
        "as_file_name": "some_test.csv",
        "schemas": [
            {
                "title": "SomeTest",
                "properties": {
                    "num": {"type": "number"},
                    "i": {"type": "integer"},
                    "arr": {"type": "array", "items": {"type": "string"}}
                 }
            }
        ],
        "expected": {
            "SomeTest": [
                {
                    "uuid": "some-uuid-a",
                    "status": "public",
                    "principals": {"view": "pav-a", "edit": "pae-a"},
                    "extensions": ["alfa", "bravo", "charlie"],
                    "num": 123.4,
                    "i": 617,
                    "arr": ["hotel"]
                },
                {
                    "uuid": "some-uuid-b",
                    "status": "public",
                    "principals": {"view": "pav-b", "edit": "pae-b"},
                    "extensions": ["delta", "echo", "foxtrot", "golf"],
                    "num": 987,
                    "i": 781,
                    "arr": ["indigo|juliet", "kilo"]
                }
            ]
        }
    },
    # ----------------------------------------------------------------------------------------------
    {
        "rows": [r"abcdef", r"alfa", r"bravo"],
        "as_file_name": "easy_test.csv",
        "noschemas": True,
        "expected": {"EasyTest": [{"abcdef": "alfa"}, {"abcdef": "bravo"}]}
    },
    # ----------------------------------------------------------------------------------------------
    {
        "rows": [
            r"abcdef,ghi.jk,l,mno#,ghi.xyzzy,foo#notaninteger",
            r"alfa,bravo,123,delta|echo|foxtrot,xyzzy:one,mike",
            r"golf,hotel,456,juliet|kilo|lima,xyzzy:two,november"
        ],
        "as_file_name": "easy_test1.csv",
        "expected": {
            "EasyTest1": [
                {
                    "abcdef": "alfa",
                    "ghi": {"jk": "bravo", "xyzzy": "xyzzy:one"},
                    "l": "123",
                    "mno": ["delta", "echo", "foxtrot"],
                    "foo#notaninteger": "mike"
                },
                {
                    "abcdef": "golf",
                    "ghi": {"jk": "hotel", "xyzzy": "xyzzy:two"},
                    "l": "456",
                    "mno": ["juliet", "kilo", "lima"],
                    "foo#notaninteger": "november"
                }
            ]
        },
        "noschemas": True
    },
    # ----------------------------------------------------------------------------------------------
    {
        "rows": [
            r"abcdef,ghi.jk,l,mno#,ghi.xyzzy,mno#2",  # TODO: fail if mno.#0 instead of mno.#
            r"alfa,bravo,123,delta|echo|foxtrot,xyzzy:one,october",
            r"golf,hotel,456,juliet|kilo|lima,xyzzy:two,november"
        ],
        "as_file_name": "easy_test2.csv",
        "noschemas": True,
        "expected": {
            "EasyTest2": [
                {
                    "abcdef": "alfa",
                    "ghi": {"jk": "bravo", "xyzzy": "xyzzy:one"},
                    "l": "123",
                    "mno": ["delta", "echo", "october"]
                },
                {
                    "abcdef": "golf",
                    "ghi": {"jk": "hotel", "xyzzy": "xyzzy:two"},
                    "l": "456",
                    "mno": ["juliet", "kilo", "november"]
                }
            ]
        }
    },
    # ----------------------------------------------------------------------------------------------
    {
        "file": "reference_file_20231119.csv", "as_file_name": "reference_file.csv",
        "expected": "reference_file_20231119.result.json",
        "expected_refs": [
            "/FileFormat/FASTA",
            "/FileFormat/VCF",
            "/SubmissionCenter/Center1"
        ],
        "norefs": SAME_AS_EXPECTED_REFS
    },
    # ----------------------------------------------------------------------------------------------
    {
        "rows": [
            r"abcdef,ghi.jk,l,mno#,ghi.xyzzy,mno#2",  # TODO: fail if mno.#0 instead of mno.#
            r"alfa,bravo,123,delta|echo|foxtrot,xyzzy:one,october",
            r"golf,hotel,456,juliet|kilo|lima,xyzzy:two,november"
        ],
        "as_file_name": "easy_test3.csv",
        "noschemas": True,
        "expected": {
            "EasyTest3": [
                {
                    "abcdef": "alfa",
                    "ghi": {"jk": "bravo", "xyzzy": "xyzzy:one"},
                    "l": "123",
                    "mno": ["delta", "echo", "october"]
                },
                {
                    "abcdef": "golf",
                    "ghi": {"jk": "hotel", "xyzzy": "xyzzy:two"},
                    "l": "456",
                    "mno": ["juliet", "kilo", "november"]
                }
            ]
        }
    },
    # ----------------------------------------------------------------------------------------------
    {
        "file": "submission_test_file_from_doug_20231106.xlsx",
        "expected_refs": [
            "/Consortium/smaht",
            "/Software/SMAHT_SOFTWARE_FASTQC",
            "/Software/SMAHT_SOFTWARE_VEPX",
            "/FileFormat/fastq",
            "/Workflow/smaht:workflow-basic"
        ],
        "norefs": [
            "/Consortium/smaht"
        ],
        "expected": "submission_test_file_from_doug_20231106.result.json"
    },
    # ----------------------------------------------------------------------------------------------
    {
        "file": "submission_test_file_from_doug_20231130.xlsx",
        "norefs": ["/SubmissionCenter/smaht_dac"],
        "expected": {
            "Donor": [{
                "submitted_id": "XY_DONOR_ABCD",
                "sex": "Female", "age": 5,
                "submission_centers": ["smaht_dac"],
                "something": "else"
            }]
        },
        "expected_errors": [
            {"src": {"type": "Donor", "row": 1},
             "error": "Validation error at '$': Additional properties are not allowed ('something' was unexpected)"}
        ]
    },
    # ----------------------------------------------------------------------------------------------
    {
        "novalidate": True,
        "file": "test_uw_gcc_colo829bl_submission_20231117.xlsx",
        "expected": "test_uw_gcc_colo829bl_submission_20231117.result.json",
        "expected_refs": [
            "/Analyte/UW-GCC_ANALYTE_COLO-829BLT-50to1_1_FiberSeq_1",
            "/Analyte/UW-GCC_ANALYTE_COLO-829BLT-50to1_1_HMWgDNA_1",
            "/Analyte/UW-GCC_ANALYTE_COLO-829BLT-50to1_1_bulkKinnex_1",
            "/Analyte/UW-GCC_ANALYTE_COLO-829BLT-50to1_1_gDNA_1",
            "/Analyte/UW-GCC_ANALYTE_COLO-829BL_FiberSeq_1",
            "/Analyte/UW-GCC_ANALYTE_COLO-829BL_FiberSeq_2",
            "/Analyte/UW-GCC_ANALYTE_COLO-829BL_HMWgDNA_1",
            "/Analyte/UW-GCC_ANALYTE_COLO-829BL_HiC_1",
            "/Analyte/UW-GCC_ANALYTE_COLO-829BL_bulkKinnex_1",
            "/Analyte/UW-GCC_ANALYTE_COLO-829BL_gDNA_1",
            "/Analyte/UW-GCC_ANALYTE_COLO-829T_FiberSeq_1",
            "/Analyte/UW-GCC_ANALYTE_COLO-829T_FiberSeq_2",
            "/Analyte/UW-GCC_ANALYTE_COLO-829T_HMWgDNA_1",
            "/Analyte/UW-GCC_ANALYTE_COLO-829T_HMWgDNA_2",
            "/Analyte/UW-GCC_ANALYTE_COLO-829T_HiC_2",
            "/Analyte/UW-GCC_ANALYTE_COLO-829T_bulkKinnex_2",
            "/Analyte/UW-GCC_ANALYTE_COLO-829T_gDNA_2",
            "/FileFormat/BAM",
            "/FileSet/UW-GCC_FILE-SET_COLO-829BL_FIBERSEQ_1",
            "/FileSet/UW-GCC_FILE-SET_COLO-829BL_FIBERSEQ_2",
            "/FileSet/UW-GCC_FILE-SET_COLO-829T_FIBERSEQ_1",
            "/Library/UW-GCC_LIBRARY_COLO-829BL_FIBERSEQ_1",
            "/Library/UW-GCC_LIBRARY_COLO-829BL_FIBERSEQ_2",
            "/Library/UW-GCC_LIBRARY_COLO-829T_FIBERSEQ_1",
            "/Sequencing/UW-GCC_SEQUENCING_PACBIO-HIFI-150x",
            "/Sequencing/UW-GCC_SEQUENCING_PACBIO-HIFI-60x",
            "/Software/UW-GCC_SOFTWARE_FIBERTOOLS-RS",
            "/UnalignedReads/<null>"
        ],
        "norefs": [
            "/FileFormat/BAM",
            "/FileSet/UW-GCC_FILE-SET_COLO-829T_FIBERSEQ_1"
        ]
    },
    # ----------------------------------------------------------------------------------------------
    {
        # Same as test_uw_gcc_colo829bl_submission_20231117.xlsx but with the blank line in the
        # Unaligned Reads sheet that signaled the end of input, and the following comment, removed.
        "file": "test_uw_gcc_colo829bl_submission_20231117_more_unaligned_reads.xlsx",
        "novalidate": True,
        "expected": "test_uw_gcc_colo829bl_submission_20231117_more_unaligned_reads.result.json",
        "expected_refs": [
            "/Analyte/UW-GCC_ANALYTE_COLO-829BLT-50to1_1_FiberSeq_1",
            "/Analyte/UW-GCC_ANALYTE_COLO-829BLT-50to1_1_HMWgDNA_1",
            "/Analyte/UW-GCC_ANALYTE_COLO-829BLT-50to1_1_bulkKinnex_1",
            "/Analyte/UW-GCC_ANALYTE_COLO-829BLT-50to1_1_gDNA_1",
            "/Analyte/UW-GCC_ANALYTE_COLO-829BL_FiberSeq_1",
            "/Analyte/UW-GCC_ANALYTE_COLO-829BL_FiberSeq_2",
            "/Analyte/UW-GCC_ANALYTE_COLO-829BL_HMWgDNA_1",
            "/Analyte/UW-GCC_ANALYTE_COLO-829BL_HiC_1",
            "/Analyte/UW-GCC_ANALYTE_COLO-829BL_bulkKinnex_1",
            "/Analyte/UW-GCC_ANALYTE_COLO-829BL_gDNA_1",
            "/Analyte/UW-GCC_ANALYTE_COLO-829T_FiberSeq_1",
            "/Analyte/UW-GCC_ANALYTE_COLO-829T_FiberSeq_2",
            "/Analyte/UW-GCC_ANALYTE_COLO-829T_HMWgDNA_1",
            "/Analyte/UW-GCC_ANALYTE_COLO-829T_HMWgDNA_2",
            "/Analyte/UW-GCC_ANALYTE_COLO-829T_HiC_2",
            "/Analyte/UW-GCC_ANALYTE_COLO-829T_bulkKinnex_2",
            "/Analyte/UW-GCC_ANALYTE_COLO-829T_gDNA_2",
            "/FileSet/UW-GCC_FILE-SET_COLO-829BL_FIBERSEQ_1",
            "/FileSet/UW-GCC_FILE-SET_COLO-829BL_FIBERSEQ_2",
            "/FileFormat/<null>",
            "/FileFormat/BAM",
            "/FileSet/UW-GCC_FILE-SET_COLO-829T_FIBERSEQ_1",
            "/Library/UW-GCC_LIBRARY_COLO-829BL_FIBERSEQ_1",
            "/Library/UW-GCC_LIBRARY_COLO-829BL_FIBERSEQ_2",
            "/Library/UW-GCC_LIBRARY_COLO-829T_FIBERSEQ_1",
            "/Sequencing/UW-GCC_SEQUENCING_PACBIO-HIFI-150x",
            "/Sequencing/UW-GCC_SEQUENCING_PACBIO-HIFI-60x",
            "/Software/UW-GCC_SOFTWARE_FIBERTOOLS-RS",
            "/UnalignedReads/<null>"
        ],
        "norefs": [
            "/FileFormat/<null>",
            "/FileFormat/BAM",
            "/FileSet/UW-GCC_FILE-SET_COLO-829T_FIBERSEQ_1"
        ]
    },
    # ----------------------------------------------------------------------------------------------
    {
        "file": "software_20231119.csv", "as_file_name": "software.csv",
        "novalidate": True,
        "expected": "software_20231119.result.json",
        "expected_refs": [
            "/Consortium/Consortium1",
            "/Consortium/Consortium2",
            "/SubmissionCenter/SubmissionCenter1",
            "/SubmissionCenter/SubmissionCenter2",
            "/User/user-id-1",
            "/User/user-id-2"
        ],
        "norefs": SAME_AS_EXPECTED_REFS
    },
    # ----------------------------------------------------------------------------------------------
    {
        "file": "workflow_20231119.csv", "as_file_name": "workflow.csv",
        "novalidate": True,
        "expected": "workflow_20231119.result.json",
        "expected_refs": [
            "/Consortium/Consortium1",
            "/Consortium/Consortium2",
            "/SubmissionCenter/SubmissionCenter1",
            "/SubmissionCenter/SubmissionCenter2",
            "/User/user-id-1",
            "/User/user-id-2"
        ],
        "norefs": SAME_AS_EXPECTED_REFS
    },
    # ----------------------------------------------------------------------------------------------
    {
        "file": "analyte_20231119.csv", "as_file_name": "analyte.csv",
        "expected": "analyte_20231119.result.json",
        "expected_refs": [
            "/Consortium/another-consortia",
            "/Consortium/smaht",
            "/Protocol/Protocol9",
            "/Sample/Sample9",
            "/SubmissionCenter/somesubctr"
        ],
        "norefs": SAME_AS_EXPECTED_REFS
    },
    # ----------------------------------------------------------------------------------------------
    {
        "file": "library_20231119.csv", "as_file_name": "library.csv",
        "expected": "library_20231119.result.json",
        "expected_refs": [
            "/Analyte/sample-analyte-1",
            "/Analyte/sample-analyte-2",
            "/Analyte/sample-analyte-3",
            "/Consortium/Consortium1",
            "/Consortium/Consortium2",
            "/LibraryPreparation/prep2",
            "/Protocol/protocol1",
            "/Protocol/protocol3",
            "/SubmissionCenter/somesubctr",
            "/SubmissionCenter/anothersubctr",
            "/SubmissionCenter/Center1",
            "/LibraryPreparation/<null>"
        ],
        "norefs": SAME_AS_EXPECTED_REFS
    },
    # ----------------------------------------------------------------------------------------------
    {
        "file": "file_format_20231119.csv.gz", "as_file_name": "file_format.csv.gz",
        "expected": "file_format_20231119.result.json",
        "expected_refs": [
            "/Consortium/358aed10-9b9d-4e26-ab84-4bd162da182b",
            "/SubmissionCenter/9626d82e-8110-4213-ac75-0a50adf890ff",
        ],
        "norefs": SAME_AS_EXPECTED_REFS
    },
    # ----------------------------------------------------------------------------------------------
    {
        "file": "cell_line_20231120.csv",
        "as_file_name": "cell_line.csv",
        "expected": "cell_line_20231120.result.json",
        "expected_refs": [
            "/SubmissionCenter/some-submission-center-a",
            "/SubmissionCenter/some-submission-center-b"
        ],
        "norefs": SAME_AS_EXPECTED_REFS
    },
    # ----------------------------------------------------------------------------------------------
    {
        "file": "unaligned_reads_20231120.csv", "as_file_name": "unaligned_reads.csv",
        "expected": "unaligned_reads_20231120.result.json",
        "expected_refs": [
            "/FileSet/FileSet1", "/FileSet/FileSet2", "/FileSet/FileSet3",
            "/QualityMetric/QC1", "/QualityMetric/QC2", "/QualityMetric/QC3",
            "/QualityMetric/QC4", "/QualityMetric/QC5", "/QualityMetric/QC6",
            "/Software/Software1", "/Software/Software2", "/Software/Software3",
            "/Software/Software4", "/Software/Software5", "/Software/Software6",
            "/SubmissionCenter/Center1", "/SubmissionCenter/Center2", "/SubmissionCenter/Center3", "/User/User1",
            "/User/User2", "/User/User3", "/User/User4", "/User/User5", "/User/User6",
            "/FileFormat/BAM", "/FileFormat/CRAM", "/FileFormat/FASTQ"
        ],
        "norefs": SAME_AS_EXPECTED_REFS
    },
    # ----------------------------------------------------------------------------------------------
    {
        "file": "sequencing_20231120.csv",
        "as_file_name": "sequencing.csv",
        "expected": "sequencing_20231120.result.json",
        "expected_refs": [
            "/Consortium/Consortium1",
            "/Consortium/Consortium2",
            "/Protocol/Protocol1",
            "/Protocol/Protocol2",
            "/Protocol/Protocol3",
            "/SubmissionCenter/Center1",
            "/SubmissionCenter/Center2",
            "/SubmissionCenter/somesubctr",
            "/User/User1",
            "/User/User2",
            "/User/User3",
            "/User/User4",
            "/User/User5",
            "/User/User6"
        ],
        "norefs": SAME_AS_EXPECTED_REFS
    },
    # ----------------------------------------------------------------------------------------------
    {
        "rows": [
            "abc#,abc#",
            "alice|bob|charley,foobar|goobar"
        ],
        "as_file_name": "test.csv",
        "noschemas": True,
        "expected": {
            "Test": [{"abc": ["foobar", "goobar"]}]
        }
    },
    # ----------------------------------------------------------------------------------------------
    {
        "rows": [
            "abc#,abc#1",  # TODO: fail if abc#.0 rather than abc#
            "alice|bob|charley,foobar"
        ],
        "as_file_name": "test.csv",
        "noschemas": True,
        "expected": {
            "Test": [{
                "abc": ["alice", "foobar", "charley"]
            }]
        }
    },
    # ----------------------------------------------------------------------------------------------
    {
        "rows": [
            "abc#,abc#",  # TODO: fail if abce#0 rather than abce#
            "alice|bob|charley,foobar|goobar"
        ],
        "as_file_name": "test.csv",
        "noschemas": True,
        "expected": {
            "Test": [{
                "abc": ["foobar", "goobar"]
            }]
        }
    },
    # ----------------------------------------------------------------------------------------------
    {
        "rows": [
            "other_allowed_extensions#,other_allowed_extensions#4",
            # "alice|bob|charley,foobar|goobar"
            "alice|bob|charley,foobar"
        ],
        "as_file_name": "test.csv",
        "noschemas": True,
        "expected": {
            "Test": [{
                # "other_allowed_extensions": ["alice", "bob", "charley", None, "foobar", "goobar"]
                "other_allowed_extensions": ["alice", "bob", "charley", None, "foobar"]
             }]
        }
    },
    # ----------------------------------------------------------------------------------------------
    {
        "rows": [
            # TODO: fail if other_allowed_extensions#0 rather than other_allowed_extensions#
            "other_allowed_extensions#,other_allowed_extensions#4",
            # "alice|bob|charley,foobar|goobar"
            "alice|bob|charley,foobar"
        ],
        "as_file_name": "test.csv",
        "noschemas": True,
        "expected": {
            "Test": [{
                # "other_allowed_extensions": ["alice", "bob", "charley", None, "foobar", "goobar"]
                "other_allowed_extensions": ["alice", "bob", "charley", None, "foobar"]
             }]
        }
    },
    # ----------------------------------------------------------------------------------------------
    {
        "rows": [
            "uuid,status,principals_allowed. view,principals_allowed.edit,"
            "other_allowed_extensions#,other_allowed_extensions#5",
            # "some-uuid-a,public,pav-a,pae-a,alice|bob|charley,foobar|goobar",
            # "some-uuid-b,public,pav-b,pae-a,alice|bob|charley,foobar|goobar"
            "some-uuid-a,public,pav-a,pae-a,alice|bob|charley,goobar",
            "some-uuid-b,public,pav-b,pae-a,alice|bob|charley,goobar"
        ],
        "as_file_name": "test.csv",
        "noschemas": True,
        "expected": {
            "Test": [
                {
                    "uuid": "some-uuid-a",
                    "status": "public",
                    "principals_allowed": {"view": "pav-a", "edit": "pae-a"},
                    # "other_allowed_extensions": ["alice", "bob", "charley", None, "foobar", "goobar"]
                    "other_allowed_extensions": ["alice", "bob", "charley", None, None, "goobar"]
                },
                {
                    "uuid": "some-uuid-b",
                    "status": "public",
                    "principals_allowed": {"view": "pav-b", "edit": "pae-a"},
                    # "other_allowed_extensions": ["alice", "bob", "charley", None, "foobar", "goobar"]
                    "other_allowed_extensions": ["alice", "bob", "charley", None, None, "goobar"]
                }
            ]
        }
    },
    # ----------------------------------------------------------------------------------------------
    {
        "rows": [
            "abc.def,pqr,vw#.xy",
            "alpha,1234,781"
        ],
        "as_file_name": "test.csv",
        "noschemas": True,
        "expected": {
            "Test": [
                {"abc": {"def": "alpha"}, "pqr": "1234", "vw": [{"xy": "781"}]}
             ]
        }
    },
    # ----------------------------------------------------------------------------------------------
    {
        "rows": [
            "xyzzy#1",
            "456"
        ],
        "as_file_name": "test.csv",
        "noschemas": True,
        "expected": {
            "Test": [
                {"xyzzy": [None, "456"]}
             ]
        }
    },
    # ----------------------------------------------------------------------------------------------
    {
        "rows": [
            "xyzzy#2",
            "456"
        ],
        "as_file_name": "test.csv",
        "noschemas": True,
        "expected": {
            "Test": [
                {"xyzzy": [None, None, "456"]}
            ]
        }
    },
    # ----------------------------------------------------------------------------------------------
    {
        "rows": [
            "abc.def.ghi,xyzzy#2",
            "123,456"
        ],
        "as_file_name": "test.csv",
        "noschemas": True,
        "expected": {
            "Test": [
                {"abc": {"def": {"ghi": "123"}}, "xyzzy": [None, None, "456"]}
             ]
        }
    },
    # ----------------------------------------------------------------------------------------------
    {
        "rows": [
            "prufrock#",
            "J.|Alfred|Prufrock"
        ],
        "as_file_name": "test.csv",
        "noschemas": True,
        "expected": {
            "Test": [
                {"prufrock": ["J.", "Alfred", "Prufrock"]}
             ]
        }
    },
    # ----------------------------------------------------------------------------------------------
    {
        "rows": [
            "abc.def,pqr,vw#1.xy",
            "alpha,1234,781"
        ],
        "as_file_name": "test.csv",
        "noschemas": True,
        "expected": {
            "Test": [
                {"abc": {"def": "alpha"}, "pqr": "1234", "vw": [{"xy": None}, {"xy": "781"}]}
             ]
        },
        "prune": False
    },
    # ----------------------------------------------------------------------------------------------
    {
        "rows": [
            "abc.def,pqr,vw#0.xy",
            "alpha,1234,781"
        ],
        "as_file_name": "test.csv",
        "noschemas": True,
        "expected": {
            "Test": [
                {"abc": {"def": "alpha"}, "pqr": "1234", "vw": [{"xy": "781"}]}
             ]
        }
    },
    # ----------------------------------------------------------------------------------------------
    {
        "rows": [
            "abc.def,pqr,vw#2.xy",
            "alpha,1234,781"
        ],
        "as_file_name": "test.csv",
        "noschemas": True,
        "expected": {
            "Test": [
                {"abc": {"def": "alpha"}, "pqr": "1234", "vw": [{"xy": None}, {"xy": None}, {"xy": "781"}]}
             ]
        },
        "prune": False
    },
    # ----------------------------------------------------------------------------------------------
    {
        "rows": [
            "vw#.xy.foo,simple_string",
            "781,moby"
        ],
        "as_file_name": "test.csv",
        "noschemas": True,
        "expected": {
            "Test": [
                {"vw": [{"xy": {"foo": "781"}}], "simple_string": "moby"}
             ]
        }
    },
    # ----------------------------------------------------------------------------------------------
    {
        "rows": [
            "abc.def,pqr,vw#.xy",
            "alpha,1234,781"
        ],
        "as_file_name": "some_type_one.csv",
        "schemas": [_load_json_from_file("some_type_one.json")],
        "expected": {
            "SomeTypeOne": [
                {"abc": {"def": "alpha"}, "pqr": 1234, "vw": [{"xy": 781}]}
             ]
        }
    },
    # ----------------------------------------------------------------------------------------------
    {
        "rows": [
            "vw#2.xy.foo",
            "781"
        ],
        "as_file_name": "some_type_two.csv",
        "schemas": [_load_json_from_file("some_type_two.json")],
        "expected": {
            "SomeTypeTwo": [
                {"vw": [
                    {"xy": {"foo": None}},
                    {"xy": {"foo": None}},
                    {"xy": {"foo": "781"}}
                ]}
             ]
        },
        "prune": False
    },
    # ----------------------------------------------------------------------------------------------
    {
        "rows": [
            "simple_string_array,simple_integer_array,simple_number_array,simple_boolean_array",
            "1|23|456|7890 , 1|23|456|7890  ,  1|23|456|7890.123 , true| False|false|True"
        ],
        "as_file_name": "some_type_one.csv",
        "schemas": [_load_json_from_file("some_type_one.json")],
        "expected": {
            "SomeTypeOne": [
                {"simple_string_array": ["1", "23", "456", "7890"],
                 "simple_integer_array": [1, 23, 456, 7890],
                 "simple_number_array": [1, 23, 456, 7890.123],
                 "simple_boolean_array": [True, False, False, True]}
            ]
        }
    },
    # ----------------------------------------------------------------------------------------------
    {
        "rows": [
            "simplearray#4\tsimplearray#\tsomeobj.ghi\tabc\tarrayofarray\tsimplearray#3",
            "hello\tabc|def|ghi\t[{\"jkl\": \"xyz\"}]\t{\"hello\": 1234}\t[[\"j.\", \"alfred\", \"prufrock\"]]\tbyebye"
            # "arrayofarray\tsimplearray#4\tsimplearray\tsomeobj.ghi\tabc\tsimplearray#3",
            # "[[\"j.\", \"alfred\", \"prufrock\"]]\thello\tabc|def|ghi\t[{\"jkl\":
            #     \"xyz\"}]\t{\"hello\": 1234}\tbyebye"
        ],
        "as_file_name": "test.tsv",
        "schemas": [_load_json_from_file("some_type_three.json")],
        "expected": {
            "Test": [
                {
                    "simplearray": ["abc", "def", "ghi", "byebye", "hello"],
                    # "simplearray": ["abc", "def", "ghi", "byebye"],
                    "abc": {"hello": 1234},
                    "someobj": {"ghi": [{"jkl": "xyz"}]},
                    "arrayofarray": [["j.", "alfred", "prufrock"]]
                    # "arrayofarray": [[["j.", "alfred", "prufrock"]]]  # TODO
                }
                # {
                #    "simplearray': ['abc', 'def', 'ghi', ['byebye']],
                #    'abc': {'hello': 1234},
                #    'someobj': {'ghi': [{'jkl': 'xyz'}]},
                #    'arrayofarray': [['j.', 'alfred', 'prufrock']]}
            ]
        }
    },
    # ----------------------------------------------------------------------------------------------
    {
        "rows": [
            "somearray#,somearray#3,somearray#4",
            "alice|bob|charley,,goobar"
        ],
        "as_file_name": "test.csv",
        "schemas": [
            {
                "title": "Test",
                "properties": {
                    "somearray": {"type": "array", "items": {"type": "string"}}
                }
            }
        ],
        "expected": {
            "Test": [{'somearray': ['alice', 'bob', 'charley', '', 'goobar']}]
        }
    },
    # ----------------------------------------------------------------------------------------------
    {
        "rows": [
            "somearray#,somearray#3,somearray#4",
            "123|456|789,0,203"
        ],
        "as_file_name": "test.csv",
        "schemas": [
            {
                "title": "Test",
                "properties": {
                    "somearray": {"type": "array", "items": {"type": "integer"}}
                }
            }
        ],
        "expected": {
            "Test": [{'somearray': [123, 456, 789, 0, 203]}]
        }
    },
    # ----------------------------------------------------------------------------------------------
    {
        "rows": [
            "arrayofarrayofobject",
            "[[{\"name\": \"prufrock\", \"id\": 1234}]]"
        ],
        "as_file_name": "test.tsv",
        "schemas": [
            {
                "title": "Test",
                "properties": {
                    "arrayofarrayofobject": {
                        "type": "array",
                        "items": {
                            "type": "array",
                            "items": {
                                "type": "object",
                                "properties": {
                                    "name": {"type": "string"},
                                    "id": {"type": "integer"}
                                }
                            }
                        }
                    }
                }
            }
        ],
        "expected": {"Test": [{"arrayofarrayofobject": [[{"name": "prufrock", "id": 1234}]]}]}
    },
    # ----------------------------------------------------------------------------------------------
    {
        "rows": [
            "arrayofobject",
            "[{\"name\": \"prufrock\", \"id\": 1234}]"
        ],
        "as_file_name": "test.tsv",
        "schemas": [
            {
                "title": "Test",
                "properties": {
                    "arrayofobject": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "name": {"type": "string"},
                                "id": {"type": "integer"}
                            }
                        }
                    }
                }
            }
        ],
        "expected": {"Test": [{"arrayofobject": [{"name": "prufrock", "id": 1234}]}]}
    },
    # ----------------------------------------------------------------------------------------------
    {
        "rows": [
            "arrayofobject#4.name,arrayofobject#4.id,arrayofobject#2.name,arrayofobject#2.id",
            "anastasiia,1234,olha,5678"
        ],
        "as_file_name": "test.csv",
        "schemas": [
            {
                "title": "Test",
                "properties": {
                    "arrayofobject": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "name": {"type": "string"},
                                "id": {"type": "integer"}
                            }
                        }
                    }
                }
            }
        ],
        # "expected": {"Test": [{"arrayofobject": [{"name": "anastasiia", "id": 1234}]}]},
        "expected": {"Test": [{"arrayofobject": [{}, {}, {"name": "olha", "id": 5678},
                                                 {}, {"name": "anastasiia", "id": 1234}]}]}
    },
    # ----------------------------------------------------------------------------------------------
    {
        "rows": [
            "arrayofarrayofobject##.name,arrayofarrayofobject##.id",
            "anastasiia,1234"
        ],
        "as_file_name": "test.csv",
        "schemas": [
            {
                "title": "Test",
                "properties": {
                    "arrayofarrayofobject": {
                        "type": "array",
                        "items": {
                            "type": "array",
                            "items": {
                                "type": "object",
                                "properties": {
                                    "name": {"type": "string"},
                                    "id": {"type": "integer"}
                                }
                            }
                        }
                    }
                }
            }
        ],
        "expected": {"Test": [{"arrayofarrayofobject": [[{"name": "anastasiia", "id": 1234}]]}]}
    },
    # ----------------------------------------------------------------------------------------------
    {
        "rows": [
            "arrayofarrayofobject##.name,arrayofarrayofobject##.id,"
            "arrayofarrayofobject##1.name,arrayofarrayofobject##1.id",
            "anastasiia,1234,olha,5678"
        ],
        "as_file_name": "test.csv",
        "schemas": [
            {
                "title": "Test",
                "properties": {
                    "arrayofarrayofobject": {
                        "type": "array",
                        "items": {
                            "type": "array",
                            "items": {
                                "type": "object",
                                "properties": {
                                    "name": {"type": "string"},
                                    "id": {"type": "integer"}
                                }
                            }
                        }
                    }
                }
            }
        ],
        "expected": {
            "Test": [
                {
                    "arrayofarrayofobject": [
                        [
                            {
                                "name": "anastasiia",
                                "id": 1234
                            },
                            {
                                "name": "olha",
                                "id": 5678
                            }
                        ]
                    ]
                }
            ]
        }
    },
    # ----------------------------------------------------------------------------------------------
    {
        "rows": [
            "indigo\tjuliet\talfa.bravo\talfa.bravo.charlie.delta",
            "abc|def|ghi|123456890\t[[[0],[12,34],[5],[67,8,90]],[[123]]]\t{\"foo\": 123}",
            "prufrock|j.|alfred|\t\t\thellocharlie",
        ],
        "as_file_name": "some_type_four.tsv",
        "schemas": [_load_json_from_file("some_type_four.json")],
        "expected": {
            "SomeTypeFour": [
                {
                    "indigo": ["abc", "def", "ghi", "123456890"],
                    "juliet": [[[0], [12, 34], [5], [67, 8, 90]], [[123]]],
                    "alfa": {"bravo": {"foo": 123}}
                },
                {
                    "indigo": ["prufrock", "j.", "alfred"],
                    "juliet": [[[]]],
                    "alfa": {"bravo": {"charlie": {"delta": "hellocharlie"}}}
                }
             ]
        },
        "expected_errors": [{'src': {'type': 'SomeTypeFour', 'row': 1},
                             'error': "Validation error at '$.alfa.bravo': {'foo': 123} is not of type 'string'"},
                            {'src': {'type': 'SomeTypeFour', 'row': 2},
                             'error': "Validation error at '$.alfa.bravo': "
                                      "{'charlie': {'delta': 'hellocharlie'}} is not of type 'string'"}]  # noqa
    },
    # ----------------------------------------------------------------------------------------------
    {
        "ignore": True,
        "rows": [
            "abc,abc##",
            "123,456",
        ],
        "as_file_name": "test.csv",
        "noschemas": True,
        "expected": {
            "Test": [
                {
                    "abc": ["456"]
                }
             ]
        }
    },
    # ----------------------------------------------------------------------------------------------
    {
        "rows": [
            "someuniquestrings,someuniqueints",
            "somevalue|anothervalue|somevalue,12|34|56|34"
        ],
        "as_file_name": "test.csv",
        "schemas": [{"title": "Test",
            "properties": {  # noqa
                "someuniquestrings": {"type": "array", "uniqueItems": True, "items": {"type": "string"}},
                "someuniqueints": {"type": "array", "uniqueItems": True, "items": {"type": "integer"}}
            }
        }],
        "expected": {"Test": [
            {"someuniquestrings": ["somevalue", "anothervalue"], "someuniqueints": [12, 34, 56]}
        ]}
    },
    # ----------------------------------------------------------------------------------------------
    {
        "rows": [
            "someproperty",
            "somevalue"
        ],
        "as_file_name": "test.csv",
        "schemas": [{"title": "Test",
            "properties": {  # noqa
                "someproperty": {"type": "string"},
                "submission_centers": {"type": "array", "uniqueItems": True, "items": {"type": "string"}}
            }
        }],
        "autoadd": {"submission_centers": ["somesubmissioncenter", "anothersubmissioncenter"]},
        "expected": {"Test": [
            {"someproperty": "somevalue", "submission_centers": ["somesubmissioncenter", "anothersubmissioncenter"]}
        ]}
    },
]))
def test_parse_structured_data_parameterized(kwargs):
    _test_parse_structured_data(testapp, **kwargs)


@pytest.mark.parametrize("columns, expected", [
    ["abc", {
        "abc": None
    }],
    ["abc,def.ghi,jkl#.mnop#2.rs", {
        "abc": None,
        "def": {"ghi": None},
        "jkl": [{"mnop": [{"rs": None}, {"rs": None}, {"rs": None}]}]
    }],
    ["abc,def,ghi.jkl,mnop.qrs.tuv", {
        "abc": None,
        "def": None,
        "ghi": {"jkl": None},
        "mnop": {"qrs": {"tuv": None}}
    }],
    ["abc,def,ghi.jkl,mnop.qrs.tuv", {
        "abc": None,
        "def": None,
        "ghi": {"jkl": None},
        "mnop": {"qrs": {"tuv": None}}
    }],
    ["abc,def.ghi,jkl#", {
       "abc": None,
       "def": {"ghi": None},
       "jkl": []
    }],
    ["abc,def.ghi,jkl#.mnop", {
        "abc": None,
        "def": {"ghi": None},
        "jkl": [{"mnop": None}]
    }],
    ["abc,def.ghi,jkl#.mnop#", {
        "abc": None,
        "def": {"ghi": None},
        "jkl": [{"mnop": []}]
    }],
    ["abc,def.ghi,jkl#.mnop#2", {
        "abc": None,
        "def": {"ghi": None},
        "jkl": [{"mnop": [None, None, None]}]
    }],
    ["abc,def.ghi,jkl#.mnop#2.rs", {
        "abc": None,
        "def": {"ghi": None},
        "jkl": [{"mnop": [{"rs": None}, {"rs": None}, {"rs": None}]}]
    }],
    ["abc,def.ghi,jkl#.mnop#2.rs#.tuv", {
        "abc": None,
        "def": {"ghi": None},
        "jkl": [{"mnop": [{"rs": [{"tuv": None}]}, {"rs": [{"tuv": None}]}, {"rs": [{"tuv": None}]}]}]
    }],
    ["abc.def.ghi,xyzzy", {
        "abc": {"def": {"ghi": None}},
        "xyzzy": None
    }],
    ["abc.def.ghi,xyzzy#,simple_string", {
        "abc": {"def": {"ghi": None}},
        "xyzzy": [],
        "simple_string": None
    }],
    ["abc.def.ghi,xyzzy#2", {
        "abc": {"def": {"ghi": None}},
        "xyzzy": [None, None, None]
    }],
    ["abc.def.ghi,xyzzy#2,xyzzy#3", {
        "abc": {"def": {"ghi": None}},
        "xyzzy": [None, None, None, None]
    }],
    ["abc.def.ghi,xyzzy#2,xyzzy#", {
        "abc": {"def": {"ghi": None}},
        "xyzzy": [None, None, None]
    }],
    ["abc.def.ghi,xyzzy#2,xyzzy#0", {
        "abc": {"def": {"ghi": None}},
        "xyzzy": [None, None, None]
    }],
    ["abc.def.ghi,xyzzy#2,xyzzy#1", {
        "abc": {"def": {"ghi": None}},
        "xyzzy": [None, None, None]
    }],
    ["abc.def.ghi,xyzzy#2,xyzzy#1.foo", {
        "abc": {"def": {"ghi": None}},
        "xyzzy": [{"foo": None}, {"foo": None}, {"foo": None}]
    }],
    ["abc.def.ghi,xyzzy#2.goo,xyzzy#1.foo", {
        "abc": {"def": {"ghi": None}},
        "xyzzy": [{"foo": None, "goo": None}, {"foo": None, "goo": None}, {"foo": None, "goo": None}]
    }],
    ["abc.def.ghi,xyzzy#2.goo,xyzzy#1.foo#", {
        "abc": {"def": {"ghi": None}},
        "xyzzy": [{"foo": [], "goo": None}, {"foo": [], "goo": None}, {"foo": [], "goo": None}]
    }],
    ["abc.def.ghi,xyzzy#2.goo,xyzzy#1.foo#0", {
        "abc": {"def": {"ghi": None}},
        "xyzzy": [{"foo": [None], "goo": None}, {"foo": [None], "goo": None}, {"foo": [None], "goo": None}]
    }],
    ["abc.def.ghi,xyzzy#2.goo,xyzzy#1.foo#2,jklmnop#3", {
        "abc": {"def": {"ghi": None}},
        "xyzzy": [{"foo": [None, None, None], "goo": None},
                  {"foo": [None, None, None], "goo": None}, {"foo": [None, None, None], "goo": None}],
        "jklmnop": [None, None, None, None]
    }],
    ["abc.def.ghi,xyzzy#2.goo,xyzzy#1.foo#2,jklmnop#3", {
        "abc": {"def": {"ghi": None}},
        "xyzzy": [{"foo": [None, None, None], "goo": None},
                  {"foo": [None, None, None], "goo": None}, {"foo": [None, None, None], "goo": None}],
        "jklmnop": [None, None, None, None]
    }],
    ["abc#", {
        "abc": []
    }],
    ["abc#0", {
        "abc": [None]
    }],
    ["abc##", {
        "abc": [[]]
    }],
    ["abc###", {
        "abc": [[[]]]
    }],
    ["abc#0#", {
        "abc": [[]]
    }],
    ["abc##0", {
        "abc": [[None]]
    }],
    ["abc#1#", {
        "abc": [[], []]
    }],
    ["abc#.name", {
        "abc": [{"name": None}]
    }],
    ["abc#0.name", {
        "abc": [{"name": None}]
    }],
    ["abc#1#2", {
       "abc": [[None, None, None], [None, None, None]]
    }],
    ["abc#1#2.id", {
       "abc": [[{"id": None}, {"id": None}, {"id": None}], [{"id": None}, {"id": None}, {"id": None}]]
    }],
    ["abc#1#2.id#", {
       "abc": [[{"id": []}, {"id": []}, {"id": []}], [{"id": []}, {"id": []}, {"id": []}]]
    }],
    ["abc#1#2.id#1", {
       "abc": [[{"id": [None, None]}, {"id": [None, None]},
                {"id": [None, None]}], [{"id": [None, None]}, {"id": [None, None]}, {"id": [None, None]}]]
    }],
    ["abc#1#2.id#1.name", {
       "abc": [[{"id": [{"name": None}, {"name": None}]},
                {"id": [{"name": None}, {"name": None}]}, {"id": [{"name": None}, {"name": None}]}],
               [{"id": [{"name": None}, {"name": None}]},
                {"id": [{"name": None}, {"name": None}]}, {"id": [{"name": None}, {"name": None}]}]]
    }]
])
def test_structured_row_data_0(columns, expected):
    _test_structured_row_data(columns, expected)


@pytest.mark.parametrize("columns, expected", [
    ["abc.def.ghi,xyzzy#2,xyzzy#", {
        "abc": {"def": {"ghi": None}},
        "xyzzy": [None, None, None]
    }]
])
def test_structured_row_data_debugging(columns, expected):
    _test_structured_row_data(columns, expected)


def test_flatten_schema_1():
    portal = Portal(testapp)
    schema = Schema.load_by_name("reference_file", portal=portal)
    schema_flattened_json = _get_schema_flat_typeinfo(schema)
    with open(os.path.join(TEST_FILES_DIR, "reference_file.flattened.json")) as f:
        expected_schema_flattened_json = json.load(f)
        assert schema_flattened_json == expected_schema_flattened_json


def test_portal_custom_schemas_1():
    schemas = [{"title": "Abc"}, {"title": "Def"}]
    portal = Portal(testapp, schemas=schemas)
    assert portal.get_schema("Abc") == schemas[0]
    assert portal.get_schema(" def ") == schemas[1]
    assert portal.get_schema("FileFormat") is not None


def test_get_type_name_1():
    assert Schema.type_name("FileFormat") == "FileFormat"
    assert Schema.type_name("file_format") == "FileFormat"
    assert Schema.type_name("file_format.csv") == "FileFormat"
    assert Schema.type_name("file_format.json") == "FileFormat"
    assert Schema.type_name("file_format.xls") == "FileFormat"
    assert Schema.type_name("File  Format") == "FileFormat"


def test_rationalize_column_name() -> None:
    _test_rationalize_column_name("abc#0#", "abc###", "abc#0##")
    _test_rationalize_column_name("abc.def.ghi", None, "abc.def.ghi")
    _test_rationalize_column_name("abc.def.ghi", "abc.def.ghi", "abc.def.ghi")
    _test_rationalize_column_name("abc.def", "abc.def.ghi", "abc.def")
    _test_rationalize_column_name("abc##", "abc", "abc##")
    _test_rationalize_column_name("abc", "abc##", "abc##")
    _test_rationalize_column_name("abc.def.nestedarrayofobject#1#23##343#.mno",
                                  "abc.def.nestedarrayofobject##1#24####.mno",
                                  "abc.def.nestedarrayofobject#1#23##343###.mno")
    _test_rationalize_column_name("abc.def.nestedarrayofobject#####.mno",
                                  "abc.def.nestedarrayofobject#####.mno",
                                  "abc.def.nestedarrayofobject#####.mno")


def _test_parse_structured_data(testapp,
                                file: Optional[str] = None,
                                as_file_name: Optional[str] = None,
                                rows: Optional[List[str]] = None,
                                expected: Optional[Union[dict, list]] = None,
                                expected_refs: Optional[List[str]] = None,
                                expected_errors: Optional[Union[dict, list]] = None,
                                norefs: Union[bool, List[str]] = False,
                                noschemas: bool = False,
                                novalidate: bool = False,
                                schemas: Optional[List[dict]] = None,
                                autoadd: Optional[dict] = None,
                                prune: bool = True,
                                ignore: bool = False,
                                debug: bool = False) -> None:

    if ignore:
        return
    if not file and as_file_name:
        file = as_file_name
    if not file and not rows:
        raise Exception("Must specify a file or rows for structured_data test.")
    if isinstance(expected, str):
        if os.path.exists(os.path.join(TEST_FILES_DIR, expected)):
            expected = os.path.join(TEST_FILES_DIR, expected)
        elif not os.path.exists(expected):
            raise Exception(f"Cannot find output result file for structured_data: {expected}")
        with open(expected, "r") as f:
            expected = json.load(f)
    elif not isinstance(expected, dict):
        raise Exception(f"Must specify a file name or a dictionary for structured_data test: {type(expected)}")
    if norefs is SAME_AS_EXPECTED_REFS:
        norefs = expected_refs
    if expected_refs is SAME_AS_NOREFS:
        expected_refs = norefs

    refs_actual = set()

    def assert_parse_structured_data():

        def call_parse_structured_data(file: str):
            nonlocal portal, novalidate, autoadd, prune, debug
            if debug:
                # import pdb ; pdb.set_trace()
                pass
            return parse_structured_data(file=file, portal=portal, novalidate=novalidate,
                                         autoadd=autoadd, prune=True if prune is not False else False)

        nonlocal file, expected, expected_errors, schemas, noschemas, debug
        portal = Portal(testapp, schemas=schemas) if not noschemas else None  # But see mocked_schemas.
        if rows:
            if os.path.exists(file) or os.path.exists(os.path.join(TEST_FILES_DIR, file)):
                raise Exception("Attempt to create temporary file with same name as existing test file: {file}")
            with temporary_file(name=file, content=rows) as tmp_file_name:
                structured_data_set = call_parse_structured_data(tmp_file_name)
                structured_data = structured_data_set.data
                validation_errors = structured_data_set.validation_errors
        else:
            if os.path.exists(os.path.join(TEST_FILES_DIR, file)):
                file = os.path.join(TEST_FILES_DIR, file)
            elif not os.path.exists(file):
                raise Exception(f"Cannot find input test file for structured_data: {file}")
            if as_file_name:
                with open(file, "rb" if file.endswith((".gz", ".tgz", ".tar", ".tar.gz", ".zip")) else "r") as f:
                    with temporary_file(name=as_file_name, content=f.read()) as tmp_file_name:
                        structured_data_set = call_parse_structured_data(tmp_file_name)
                        structured_data = structured_data_set.data
                        validation_errors = structured_data_set.validation_errors
            else:
                structured_data_set = call_parse_structured_data(file)
                structured_data = structured_data_set.data
                validation_errors = structured_data_set.validation_errors
        if debug:
            # import pdb ; pdb.set_trace()
            pass
        if expected is not None:
            if not (structured_data == expected):
                # import pdb ; pdb.set_trace()
                pass
            assert structured_data == expected
        if expected_errors:
            assert validation_errors == expected_errors
        else:
            assert not validation_errors

    @contextmanager
    def mocked_schemas():
        yield

    @contextmanager
    def mocked_refs():
        real_ref_exists = Portal.ref_exists
        real_map_function_ref = Schema._map_function_ref
        def mocked_map_function_ref(self, typeinfo):  # noqa
            map_ref = real_map_function_ref(self, typeinfo)
            def mocked_map_ref(value, link_to, portal, src):  # noqa
                nonlocal norefs, expected_refs, refs_actual
                if not value:
                    refs_actual.add(ref := f"/{link_to}/<null>")
                    if norefs is True or (isinstance(norefs, list) and ref in norefs):
                        return value
                return map_ref(value, src)
            return lambda value, src: mocked_map_ref(value, typeinfo.get("linkTo"), self._portal, src)
        def mocked_ref_exists(self, type_name, value):  # noqa
            nonlocal norefs, expected_refs, refs_actual
            refs_actual.add(ref := f"/{type_name}/{value}")
            if norefs is True or (isinstance(norefs, list) and ref in norefs):
                return [{"type": "dummy", "uuid": "dummy"}]
            return real_ref_exists(self, type_name, value)
        with mock.patch("dcicutils.structured_data.Portal.ref_exists",
                        side_effect=mocked_ref_exists, autospec=True):
            with mock.patch("dcicutils.structured_data.Schema._map_function_ref",
                            side_effect=mocked_map_function_ref, autospec=True):
                yield

    def run_this_function():
        nonlocal expected_refs, noschemas, norefs, refs_actual
        refs_actual = set()
        if noschemas:
            if norefs or expected_refs:
                with mocked_schemas():
                    with mocked_refs():
                        assert_parse_structured_data()
            else:
                with mocked_schemas():
                    assert_parse_structured_data()
        elif norefs or expected_refs:
            with mocked_refs():
                assert_parse_structured_data()
        else:
            assert_parse_structured_data()
        if expected_refs:
            # Make sure any/all listed refs were actually referenced.
            assert refs_actual == set(expected_refs)

    run_this_function()


def _get_schema_flat_typeinfo(schema: Schema):
    def map_function_name(map_function: Callable) -> str:
        # This is ONLY for testing/troubleshooting; get the NAME of the mapping function; this is HIGHLY
        # implementation DEPENDENT, on the map_function_<type> functions. The map_function, as a string,
        # looks like: <function Schema._map_function_string.<locals>.map_string at 0x103474900> or
        # if it is implemented as a lambda (to pass in closure), then inspect.getclosurevars.nonlocals looks like:
        # {"map_enum": <function Schema._map_function_enum.<locals>.map_enum at 0x10544cd60>, ...}
        if isinstance(map_function, Callable):
            if (match := re.search(r"\.(\w+) at", str(map_function))):
                return f"<{match.group(1)}>"
            for item in inspect.getclosurevars(map_function).nonlocals:
                if item.startswith("map_"):
                    return f"<{item}>"
        return type(map_function)

    result = {}
    for key, value in schema._typeinfo.items():
        if isinstance(value, str):
            result[key] = value
        elif isinstance(value, dict):
            key_type = value["type"]
            key_map = value.get("map")
            result[key] = {"type": key_type,
                           "map": map_function_name(key_map) if isinstance(key_map, Callable) else None}
    return result


def _test_structured_row_data(columns: str, expected: Optional[dict]):
    if _StructuredRowTemplate(columns.split(","))._template != expected:
        # import pdb ; pdb.set_trace()
        pass
    assert _StructuredRowTemplate(columns.split(","))._template == expected


def _test_rationalize_column_name(column_name: str, schema_column_name: str, expected: str) -> None:
    class FakeSchema(Schema):
        class FakeTypeInfo:
            def __init__(self, value):
                self._value = value
            def get(self, column_name):  # noqa
                return self._value
        def __init__(self, value):  # noqa
            self._typeinfo = FakeSchema.FakeTypeInfo(value)
    assert FakeSchema(schema_column_name).rationalize_column_name(column_name) == expected


# Same as in smaht-portal/../schema_formats.py
def is_accession(value: str) -> bool:
    return isinstance(value, str) and re.match(r"^SMA[1-9A-Z]{9}$", value) is not None


# Same as in smaht-portal/../ingestion_processors.py
def parse_structured_data(file: str, portal: Optional[Union[VirtualApp, TestApp, Portal]], novalidate: bool = False,
                          autoadd: Optional[dict] = None, prune: bool = True,
                          ref_nocache: bool = False) -> StructuredDataSet:

    def ref_lookup_strategy(type_name: str, schema: dict, value: str) -> (int, Optional[str]):
        not_an_identifying_property = "filename"
        if schema_properties := schema.get("properties"):
            if schema_properties.get("accession") and is_accession(value):
                # Case: lookup by accession (only by root).
                return StructuredDataSet.REF_LOOKUP_ROOT, not_an_identifying_property
            elif schema_property_info_submitted_id := schema_properties.get("submitted_id"):
                if schema_property_pattern_submitted_id := schema_property_info_submitted_id.get("pattern"):
                    if re.match(schema_property_pattern_submitted_id, value):
                        # Case: lookup by submitted_id (only by specified type).
                        return StructuredDataSet.REF_LOOKUP_SPECIFIED_TYPE, not_an_identifying_property
        return StructuredDataSet.REF_LOOKUP_DEFAULT, not_an_identifying_property

    structured_data = StructuredDataSet.load(file=file, portal=portal,
                                             autoadd=autoadd, order=ITEM_INDEX_ORDER, prune=prune,
                                             ref_lookup_strategy=ref_lookup_strategy,
                                             ref_lookup_nocache=ref_nocache)
    if not novalidate:
        structured_data.validate()
    return structured_data
