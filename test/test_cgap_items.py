import json
from pathlib import Path

from dcicutils.cgap_items import User


SOME_USER = {
    "uuid": "some-uuid",
    "email": "some-email",
    "first_name": "some-first-name",
    "last_name": "some-last-name",
    "title": "some-title",
    "project": "some-project",
}
KEYS_FILE = Path.expanduser(Path("~/.cgap-keys.json")).absolute()
USER_IDENTIFIER = "5196db84-f3d9-44bd-bba0-59e9e83634a1"


def get_keys():
    keys = json.loads(KEYS_FILE.read_text())
    return keys["msa"]


def test_user():
    auth = get_keys()
    user = User.from_identifier_and_auth(USER_IDENTIFIER, auth, embed_items=True)
    import pdb; pdb.set_trace()
    project = user.project
    assert user.uuid == USER_IDENTIFIER
