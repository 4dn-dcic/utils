from dcicutils.common import EnvName, OrchestratedApp, APP_CGAP, APP_FOURFRONT, ORCHESTRATED_APPS


def test_app_constants():

    assert set(ORCHESTRATED_APPS) == {APP_CGAP, APP_FOURFRONT} == {'cgap', 'fourfront'}

    # For thexe next two, which are really type hints, just test that they exist.
    assert EnvName
    assert OrchestratedApp
