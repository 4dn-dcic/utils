import pytest
import subprocess
import sys


@pytest.mark.skip("This test seems to break others intermittently probably because messing with numpy installation.")
def test_hack_for_elasticsearch_numpy_usage():
    try:
        subprocess.run("pip install numpy==2.0.0".split())
        for module in [module_name for module_name in sys.modules
                       if module_name.startswith("elasticsearch") or module_name.startswith("dcicutils")]:
            del sys.modules[module]
        import dcicutils.ff_utils  # noqa
    finally:
        subprocess.run("pip uninstall --yes numpy".split())
