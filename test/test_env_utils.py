import pytest

from dcicutils.env_utils import is_stg_or_prd_env, is_cgap_env, is_fourfront_env


def test_is_cgap_env():

    assert is_cgap_env('fourfront-cgap') is True
    assert is_cgap_env('cgap-prod') is True
    assert is_cgap_env('fourfront-blue') is False


def test_is_fourfront_env():

    assert is_fourfront_env('fourfront-cgap') is False
    assert is_fourfront_env('cgap-prod') is False
    assert is_fourfront_env('fourfront-blue') is True


def test_is_stg_or_prd_env():

    assert is_stg_or_prd_env("fourfront-green") is True
    assert is_stg_or_prd_env("fourfront-blue") is True
    assert is_stg_or_prd_env("fourfront-blue-1") is True
    assert is_stg_or_prd_env("fourfront-webprod") is True
    assert is_stg_or_prd_env("fourfront-webprod2") is True

    assert is_stg_or_prd_env("fourfront-yellow") is False
    assert is_stg_or_prd_env("fourfront-mastertest") is False
    assert is_stg_or_prd_env("fourfront-mastertest-1") is False
    assert is_stg_or_prd_env("fourfront-wolf") is False

    assert is_stg_or_prd_env("fourfront-cgap") is True
    assert is_stg_or_prd_env("fourfront-cgap-blue") is True
    assert is_stg_or_prd_env("fourfront-cgap-green") is True

    assert is_stg_or_prd_env("fourfront-cgap-yellow") is False
    assert is_stg_or_prd_env("fourfront-cgapwolf") is False
    assert is_stg_or_prd_env("fourfront-cgaptest") is False
