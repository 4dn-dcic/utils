from dcicutils.common import EnvName, OrchestratedApp, APP_CGAP, APP_FOURFRONT, ORCHESTRATED_APPS


def test_app_constants():

    assert set(ORCHESTRATED_APPS) == {APP_CGAP, APP_FOURFRONT} == {'cgap', 'fourfront'}


