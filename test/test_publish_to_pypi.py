from unittest import mock

from dcicutils.scripts import publish_to_pypi as publish_to_pypi_module
from dcicutils.scripts.publish_to_pypi import verify_not_already_published


def _mock_response(status_code=200, json_data=None, raise_json_error=False):
    response = mock.MagicMock()
    response.status_code = status_code
    if raise_json_error:
        response.json.side_effect = ValueError("No JSON object could be decoded")
    else:
        response.json.return_value = json_data or {}
    return response


def test_verify_not_already_published_version_exists():
    # PyPI JSON API lists the requested version among the releases -> already published.
    json_data = {"releases": {"1.0.0": [{"filename": "foo-1.0.0.tar.gz"}], "0.9.6": [{}]}}
    with mock.patch.object(publish_to_pypi_module.requests, "get",
                           return_value=_mock_response(200, json_data)) as mock_get:
        assert verify_not_already_published("encoded-core", "1.0.0") is False
        mock_get.assert_called_once_with("https://pypi.org/pypi/encoded-core/json")


def test_verify_not_already_published_version_does_not_exist():
    # Package exists on PyPI but the requested version is not among its releases -> not yet published.
    json_data = {"releases": {"0.9.6": [{"filename": "foo-0.9.6.tar.gz"}]}}
    with mock.patch.object(publish_to_pypi_module.requests, "get", return_value=_mock_response(200, json_data)):
        assert verify_not_already_published("encoded-core", "1.0.1") is True


def test_verify_not_already_published_package_does_not_exist():
    # Package has no PyPI presence at all; JSON endpoint 404s with a body lacking "releases".
    json_data = {"message": "Not Found"}
    with mock.patch.object(publish_to_pypi_module.requests, "get", return_value=_mock_response(404, json_data)):
        assert verify_not_already_published("some-brand-new-package", "0.1.0") is True


def test_verify_not_already_published_request_failure():
    # Any unexpected error talking to PyPI should not block publishing.
    with mock.patch.object(publish_to_pypi_module.requests, "get", side_effect=Exception("network error")):
        assert verify_not_already_published("encoded-core", "1.0.0") is True
