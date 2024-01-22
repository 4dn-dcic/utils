import json
import os
from dcicutils.portal_utils import Portal
from dcicutils.zip_utils import temporary_file
from unittest import mock
from .conftest_settings import TEST_DIR


_TEST_KEY_ID = "TTVJOW2A"
_TEST_SECRET = "3fbswrice6xosnjw"
_TEST_KEY = {"key": _TEST_KEY_ID, "secret": _TEST_SECRET}
_TEST_KEY_PAIR = (_TEST_KEY_ID, _TEST_SECRET)


def test_portal_constructor_a():

    def assert_for_server(server, expected):

        portal = Portal(_TEST_KEY, server=server) if server is not None else Portal(_TEST_KEY)
        assert portal.key_id == _TEST_KEY_ID
        assert portal.secret == _TEST_SECRET
        assert portal.key_pair == _TEST_KEY_PAIR
        assert portal.server == expected
        assert portal.keys_file is None
        assert portal.app is None
        assert portal.env is None
        assert portal.vapp is None

        portal = Portal({**_TEST_KEY, "server": server}) if server is not None else Portal(_TEST_KEY)
        assert portal.key_id == _TEST_KEY_ID
        assert portal.secret == _TEST_SECRET
        assert portal.key_pair == _TEST_KEY_PAIR
        assert portal.server == expected
        assert portal.keys_file is None
        assert portal.app is None
        assert portal.env is None
        assert portal.vapp is None

    assert_for_server(None, None)
    assert_for_server("http://localhost:8000", "http://localhost:8000")
    assert_for_server("http://localhost:8000/", "http://localhost:8000")
    assert_for_server("hTtP://localhost:8000//", "http://localhost:8000")
    assert_for_server("hTtP://localhost:8000//", "http://localhost:8000")
    assert_for_server("Http://xyzzy.com//abc/", "http://xyzzy.com/abc")
    assert_for_server("xhttp://localhost:8000", None)
    assert_for_server("http:/localhost:8000", None)


def test_portal_constructor_b():

    keys_file_content = json.dumps({
        "smaht-local": {
            "key": "ABCDEFGHI",
            "secret": "adfxdloiebvhzp",
            "server": "http://localhost:8080/"
        },
        "smaht-remote": {
            "key": "GHIDEFABC",
            "secret": "zpadfxdloiebvh",
            "server": "https://smaht.hms.harvard.edu/"
        }
    }, indent=4)

    with temporary_file(name=".smaht-keys.json", content=keys_file_content) as keys_file:

        portal = Portal(keys_file, env="smaht-local")
        assert portal.key_id == "ABCDEFGHI"
        assert portal.secret == "adfxdloiebvhzp"
        assert portal.key_pair == ("ABCDEFGHI", "adfxdloiebvhzp")
        assert portal.server == "http://localhost:8080"
        assert portal.key == {"key": "ABCDEFGHI", "secret": "adfxdloiebvhzp", "server": "http://localhost:8080"}
        assert portal.keys_file == keys_file
        assert portal.env == "smaht-local"
        assert portal.app is None
        assert portal.vapp is None
        assert portal.ini_file is None

        portal = Portal(keys_file, env="smaht-remote")
        assert portal.key_id == "GHIDEFABC"
        assert portal.secret == "zpadfxdloiebvh"
        assert portal.key_pair == ("GHIDEFABC", "zpadfxdloiebvh")
        assert portal.server == "https://smaht.hms.harvard.edu"
        assert portal.key == {"key": "GHIDEFABC", "secret": "zpadfxdloiebvh", "server": "https://smaht.hms.harvard.edu"}
        assert portal.keys_file == keys_file
        assert portal.env == "smaht-remote"
        assert portal.app is None
        assert portal.vapp is None
        assert portal.ini_file is None

        Portal.KEYS_FILE_DIRECTORY = os.path.dirname(keys_file)
        portal = Portal("smaht-local", app="SMaHT")
        assert portal.key_id == "ABCDEFGHI"
        assert portal.secret == "adfxdloiebvhzp"
        assert portal.key_pair == ("ABCDEFGHI", "adfxdloiebvhzp")
        assert portal.server == "http://localhost:8080"
        assert portal.key == {"key": "ABCDEFGHI", "secret": "adfxdloiebvhzp", "server": "http://localhost:8080"}
        assert portal.keys_file == keys_file
        assert portal.env == "smaht-local"
        assert portal.app == "smaht"
        assert portal.vapp is None
        assert portal.ini_file is None


def test_portal_constructor_c():

    keys_file_content = json.dumps({
        "cgap-local": {
            "key": "ABCDEFGHI",
            "secret": "adfxdloiebvhzp",
            "server": "http://localhost:8080/"
        }
    }, indent=4)

    with temporary_file(name=".cgap-keys.json", content=keys_file_content) as keys_file:

        portal = Portal(keys_file)
        assert portal.key_id == "ABCDEFGHI"
        assert portal.secret == "adfxdloiebvhzp"
        assert portal.key_pair == ("ABCDEFGHI", "adfxdloiebvhzp")
        assert portal.server == "http://localhost:8080"
        assert portal.key == {"key": "ABCDEFGHI", "secret": "adfxdloiebvhzp", "server": "http://localhost:8080"}
        assert portal.keys_file == keys_file
        assert portal.env == "cgap-local"
        assert portal.app is None
        assert portal.vapp is None
        assert portal.ini_file is None

        portal_copy = Portal(portal)
        assert portal.ini_file == portal_copy.ini_file
        assert portal.key == portal_copy.key
        assert portal.key_pair == portal_copy.key_pair
        assert portal.key_id == portal_copy.key_id
        assert portal.secret == portal_copy.secret
        assert portal.keys_file == portal_copy.keys_file
        assert portal.env == portal_copy.env
        assert portal.server == portal_copy.server
        assert portal.app == portal_copy.app
        assert portal.vapp == portal_copy.vapp
        assert portal.secret == portal_copy.secret


def test_portal_schemas_super_type_map():

    with open(f"{TEST_DIR}/data_files/sample_schemas.json") as f:
        mocked_portal_schemas = json.load(f)

    with mock.patch("dcicutils.portal_utils.Portal.get_schemas", return_value=mocked_portal_schemas):

        portal = Portal(raise_exception=False)

        assert portal.is_schema_type("UnalignedReads", "UnalignedReads") is True
        assert portal.is_schema_type("UnalignedReads", "unalignedreads") is True
        assert portal.is_schema_type("UnalignedReads", "unaligned_reads") is True
        assert portal.is_schema_type("UnalignedReads", "SubmittedFile") is True
        assert portal.is_schema_type("UnalignedReads", "SUBMITTEDfILE") is True
        assert portal.is_schema_type("UnalignedReads", "File") is True
        assert portal.is_schema_type("UnalignedReads", "file") is True

        assert portal.isinstance_schema({"@type": "UnalignedReads"}, "UnalignedReads") is True
        assert portal.isinstance_schema({"@type": "UnalignedReads"}, "UNALIGNEDREADS") is True
        assert portal.isinstance_schema({"@type": "UnalignedReads"}, "Unaligned_Reads") is True
        assert portal.isinstance_schema({"@type": "UnalignedReads"}, "SubmittedFile") is True
        assert portal.isinstance_schema({"@type": "UnalignedReads"}, "SUBMITTEDFILE") is True
        assert portal.isinstance_schema({"@type": "UnalignedReads"}, "SUBMITTED_FILE") is True
        assert portal.isinstance_schema({"@type": "SubmittedFile"}, "SUBMITTED_FILE") is True
        assert portal.isinstance_schema({"@type": "SubmittedFile"}, "File") is True
        assert portal.isinstance_schema({"@type": "foo", "data_type": "UnalignedReads"}, "UnalignedReads") is True

        assert portal.is_schema_type({"@type": "UnalignedReads"}, "UnalignedReads") is True
        assert portal.is_schema_type({"@type": "UnalignedReads"}, "UNALIGNEDREADS") is True
        assert portal.is_schema_type({"@type": "UnalignedReads"}, "Unaligned_Reads") is True
        assert portal.is_schema_type({"@type": "UnalignedReads"}, "SubmittedFile") is True
        assert portal.is_schema_type({"@type": "UnalignedReads"}, "SUBMITTEDFILE") is True
        assert portal.is_schema_type({"@type": "UnalignedReads"}, "SUBMITTED_FILE") is True
        assert portal.is_schema_type({"@type": "SubmittedFile"}, "SUBMITTED_FILE") is True
        assert portal.is_schema_type({"@type": "SubmittedFile"}, "File") is True
        assert portal.is_schema_type({"@type": "foo", "data_type": "UnalignedReads"}, "UnalignedReads") is True
