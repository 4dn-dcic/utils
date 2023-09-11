import csv
import io
import re

from dcicutils.common import (
    EnvName, OrchestratedApp, APP_CGAP, APP_FOURFRONT, APP_SMAHT, ORCHESTRATED_APPS, Regexp, CsvReader
)


def test_app_constants():

    assert set(ORCHESTRATED_APPS) == {APP_CGAP, APP_FOURFRONT, APP_SMAHT} == {'cgap', 'fourfront', 'smaht'}

    # For thexe next two, which are really type hints, just test that they exist.
    assert EnvName
    assert OrchestratedApp


def test_type_hint_regexp():

    regexp_string = "x.?y*"
    assert not isinstance(regexp_string, Regexp)
    assert isinstance(re.compile(regexp_string), Regexp)


def test_type_hint_csv_reader():

    csv_filename = "something.csv"
    open_csv_file = io.StringIO("some,csv,data")
    assert not isinstance(csv_filename, CsvReader)
    assert not isinstance(open_csv_file, CsvReader)
    assert isinstance(csv.reader(open_csv_file), CsvReader)
