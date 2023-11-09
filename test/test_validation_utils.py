import copy
import os
import pytest
import re

from dcicutils.bundle_utils import inflate
from .helpers import using_fresh_ff_state_for_testing
from dcicutils.misc_utils import AbstractVirtualApp, NamedObject, json_file_contents, to_snake_case, to_camel_case
from dcicutils.qa_utils import MockResponse, printed_output
from dcicutils.validation_utils import SchemaManager, validate_data_against_schemas, summary_of_data_validation_errors
from .conftest_settings import TEST_DIR
from .helpers_for_bundles import SAMPLE_WORKBOOK_WITH_NAME_REFS


@using_fresh_ff_state_for_testing()
def test_schema_manager_simple():

    print()  # start on a fresh line

    with printed_output() as printed:

        schema_manager_1 = SchemaManager()

        assert printed.lines == [
            "The portal_env was not explicitly supplied. Schemas will come from portal_env='data'."
        ]

        assert schema_manager_1.SCHEMA_CACHE == {}

        # Test that schema-lookup works, since that's kinda what these are about
        user_schema = schema_manager_1.fetch_schema('user')
        assert isinstance(user_schema, dict)
        assert user_schema.get('title') == 'User'

        assert schema_manager_1.override_schemas == {}


@using_fresh_ff_state_for_testing()
@pytest.mark.parametrize('schema_id', ['user', 'User'])
def test_schema_manager_with_schemas(schema_id):

    print()  # start on a fresh line

    with printed_output() as printed:

        schemas = {schema_id: {}}
        snake_id = to_snake_case(schema_id)
        camel_id = to_camel_case(schema_id)

        # Just to make sure to_snake_case and to_camel_case aren't doing something weird
        assert schema_id == snake_id or schema_id == camel_id

        schema_manager_2 = SchemaManager(override_schemas=schemas)

        # whether 'User' or 'user' was an input, it will be canonicalized to snake case
        assert schema_manager_2.override_schemas == {snake_id: {}}

        assert printed.lines == [
            "The portal_env was not explicitly supplied. Schemas will come from portal_env='data'."
        ]

        assert schema_manager_2.fetch_schema(snake_id) == {}
        assert schema_manager_2.fetch_schema(camel_id) == {}

        # even after using a camel case id, only the snake_id will be in the table
        assert schema_manager_2.override_schemas == {snake_id: {}}

        # this would only get updated if we fetched something remotely
        assert schema_manager_2.SCHEMA_CACHE == {}


@using_fresh_ff_state_for_testing()
def test_schema_manager_identifying_value():

    with pytest.raises(ValueError) as exc:
        assert SchemaManager.identifying_value({'any': 'thing'}, [], raise_exception=True)
    assert str(exc.value) == "No identifying properties were specified."

    person_named_fred = {'age': 33, 'name': 'Fred', 'favorite-color': 'yellow'}
    assert SchemaManager.identifying_value(person_named_fred, ['uuid', 'name'], raise_exception=True) == 'Fred'

    person_nicknamed_fred = {'age': 33, 'nickname': 'Fred', 'favorite-color': 'yellow'}
    with pytest.raises(ValueError) as exc:
        SchemaManager.identifying_value(person_nicknamed_fred, ['uuid', 'name'], raise_exception=True)
    assert str(exc.value) == ("""There are no identifying properties 'uuid' or 'name'"""
                              """ in {"age": 33, "nickname": "Fred", "favorite-color": "yellow"}.""")

    with pytest.raises(ValueError) as exc:
        SchemaManager.identifying_value(person_nicknamed_fred, ['name'], raise_exception=True)
    assert str(exc.value) == ("""There is no identifying property 'name'"""
                              """ in {"age": 33, "nickname": "Fred", "favorite-color": "yellow"}.""")


def test_validate_data_against_schemas():

    with SchemaManager.fresh_schema_manager_context_for_testing():

        class MockVapp(NamedObject, AbstractVirtualApp):

            @classmethod
            def get(cls, path_url):

                m = re.match('/profiles/(.*)[.]json?', path_url)
                if m:
                    base = to_snake_case(m.group(1))
                    file = os.path.join(TEST_DIR, 'data_files', 'sample_schemas', f'{base}.json')
                    response_data = json_file_contents(file)
                    response = MockResponse(200, json=response_data, url=path_url)
                    return response
                raise Exception(f"MockVapp can't handle this case: {path_url}")

        portal_vapp = MockVapp(name=f'MockVapp["data_files/sample_schemas"]')

        good_workbook = inflate(SAMPLE_WORKBOOK_WITH_NAME_REFS)

        assert validate_data_against_schemas(good_workbook, portal_vapp=portal_vapp) is None

        bogus_workbook = copy.deepcopy(good_workbook)  # modified immediately below
        user_items = bogus_workbook['User']
        user_item0 = user_items[0]
        user_item0['bogus'] = 'item'

        assert validate_data_against_schemas(bogus_workbook, portal_vapp=portal_vapp) == {
            'errors': [
                {
                    'extraneous_properties': ['bogus'],
                    'index': 0,
                    'item': 'e0dec518-cb0c-45f3-8c97-21b2659ec129',
                    'type': 'User'
                }
            ]
        }


def test_summary_of_data_validation_errors():

    error_report_1 = {
        'errors': [
            {
                'extraneous_properties': ['bogus'],
                'index': 0,
                'item': 'e0dec518-cb0c-45f3-8c97-21b2659ec129',
                'type': 'User'
            }
        ]
    }

    sample_data_file_name = 'my-data-file'
    sample_s3_data_file_location = 'my-s3-data-file-location'
    sample_s3_details_location = 'my-s3-details-location'

    assert summary_of_data_validation_errors(error_report_1,
                                             data_file_name=sample_data_file_name,
                                             s3_data_file_location=sample_s3_data_file_location,
                                             s3_details_location=sample_s3_details_location
                                             ) == [
        'Ingestion data validation error summary:',
        'Data file: my-data-file',
        'Data file in S3: my-s3-data-file-location',
        'Items unidentified: 0',
        'Items missing properties: 0',
        'Items with extraneous properties: 1',
        'Other errors: 0',
        'Exceptions: 0',
        'Details: my-s3-details-location'
    ]
