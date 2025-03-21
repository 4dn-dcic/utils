=========
dcicutils
=========

----------
Change Log
----------

8.18.3
======
* dmichaels / 2025-03-05 / branch: dmichaels-20250305-add-portal-get-schema-super-types / PR-328
  - Added method portal_utils.get_schema_super_type_names (for use by smaht-submitr).


8.18.1
======
* dmichaels / 2025-02-28 / branch: dmichaels-20250228-correct-submitr-config-path / PR-327
  - Corrected branch to smaht-submitr config dcicutils/submitr/custom_excel.py to master branch; i.e.:
    https://raw.githubusercontent.com/smaht-dac/submitr/refs/heads/master/submitr/config/custom_column_mappings.json


8.18.0
======
* dmichaels / 2025-02-11 / branch: dmichaels-20250211-submitr-custom-excel / PR-326
  - Support for qc_values pseudo-columns in smaht-submitr (and multiple sheet with same type).
    This is encapsulated in custom_excel.py, here because it is needed not only
    within smaht-submitr but also within smaht-portal (ingestion_processor.py).


8.17.0
======
* dmichaels / 2025-01-14 / branch: dmichaels-structured-data-row-mapper-hook-20250114 / PR-324
* Added hook to structured_data.StructuredDataSet to allow a custom Excel class
  to be use, so a custom column mapping can be provided; this was initially to support
  special/more-intuitive columns for QC values in the submission spreadsheet for smaht-submitr.
* Added hook to structured_data.StructuredDataSet to allow multiple sheets associated with
  the same type (via a new data_readers.Excel.effective_sheet_name function).


8.16.6
======
* dmichaels / 2025-01-10
* Fix to dcicutils/scripts/publish_to_pypy.py script. They (pypi) changed their API so this returns
  HTTP 200 even if the package version does NOT exist: https://pypi.org/project/{package_name}/{package_version}
  So without this fix this script thinks the version to publish already exists even when it does not.
  Changed to use this instead: https://pypi.org/pypi/{package_name}/json which returns JSON for ALL versions.


8.16.4
======
* dmichaels / 2024-11-17
* Very minor fix in datetime_utils.parse_datetime_string.


8.16.3
======
* dmichaels / 2024-11-08
* Minor updates to portal_utils for internal command-line utility development.


8.16.2
======
* dmichaels / 2024-10-30
* Added license exception for autocommand: GNU Lesser General Public License v3 (LGPLv3);
  see license_policies/"c4-python-infrastructure.jsonc. Ran into this with latest building of submitr.


8.16.1
======
* dmichaels / 2024-10-11
* Updated (added) version (43.0.1) for cryptography library for vulnerabilities (CVE-2023-50782, CVE-2024-0727, et.al.).
  Updated pyopenssl version (24.2.1) related to above.


8.16.0
======

* Minor changes to view_portal_object utility script.
* Minor changes to validators hooks in structured_data.
* Added portal_utils.Portal.get_version method.
* Minor fix in misc_utils.format_duration.


8.15.0
======
* 2024-10-04 (dmichaels)
* Added optional bucket argument to s3_utils.s3Utils.read_s3 (initially for Andy).


8.14.3
======

* 2024-08-22 (dmichaels)
* Modified structured_data property hook for "finish" attribute/callable.
* Added sheet hook to structured_data.
* Added portal_utils.Portal.head method.


8.14.2
======
* Corrected requests version (to ^2.27.0 from 2.31.0) on pyproject.toml to not be pinned; but doing
  so (which resolves to 2.32.3) results in this error on test/test_ecr_utils.py::test_ecr_utils_workflow:
  docker.errors.DockerException: Error while fetching server API version: Not supported URL scheme http+docker
  But upgrading the docker package (from ^4.4.4) version to ^7.1.0 fixes that.


8.14.1
======
* Minor changes to utility/troubleshooting/convenience scripts view-portal-object and update-portal-object.


8.14.0
======
* Minor updates to the view-portal-object dev/troubleshooting utility script.
* Changed from typing_extensions import Literal to import from typing;
  odd Python 3.12 issue but only in GitHub Actions (observed for submitr).
* Added tomli dependency in pyproject.toml (came up in submitr GA for Pyhthon 3.12).
* Change to structured_data.py to NOT silently convert a string
  representing a floating point number to an integer.
* Changes in structured_data.py for validator hook. 
* Added to_number function to misc_utils.
* Added run_concurrently function to misc_utils.
* Changed misc_utils.to_enum to default to non-fuzzy (prefix) match, for structured_data,
  i.e for smaht-submitr to match on (case-insensitive) full enum namess.
* Changed dcicutils.structured_data.Schema._map_function_date/time to
  report malformed dates, e.g. "6/29/2024" rather than "2024-06-29".


8.13.3
======
* N.B. Accidentially tagged/pushed 8.13.1 -> PLEASE IGNORE VERSION: 8.13.1 (subsequently yanked).
  And then to correct (while no permission to delete above) pushed unofficial 8.13.2.
* Fallout from Python 3.12 support.
  - Though dcicutils is not dependent on numpy, elasticsearch *tries* to import it,
    and if it is installed and if it is a version greater than 1.x, we get this error:
    AttributeError: `np.float_` was removed in the NumPy 2.0 release. Use `np.float64` instead.
    So added a hack in  hack_for_elasticsearch_numpy_usage.py for this specific case;
    to be imported before we import elasticsearch modules.
* Added/updated scripts from submitr: view_portal_object.py and update_portal_object.py
  for dev/troubleshooting purposes.


8.13.0
======
* Updates related to Python 3.12.
  - Had to update flake8 (from 5.0.4) to 7.1.0.
  - Had to update lower bound of Python version (from 3.8.0) to 3.8.1.
  - Had to update pyramid (from 1.10.4) to 2.0.2 (imp import not found).
  - Had to update elasticsearch (from 7.13.4) to 7.17.9 (for snovault).


8.12.0
======
* Changes related to pyinstaller experimentation for smaht-submitr.
  Mostly changing calls to exit to sys.exit; and related license_utils change.
* Added hook to structured_data (row_reader_hook) for integration testing purposes.


8.11.0
======

* Add more schema parsing functions to `schema_utils`, including for new properties for
  generating submission templates


8.10.0
======

* Added merge capabilities to structured_data.
* Added Question class to command_utils (factored out of smaht-submitr).
* Refactored out some identifying property related code from portal_object_utils to portal_utils.
* Internalized lookup_strategy related code to structured_data/portal_object_utils/portal_utils.


8.9.0
=====

* Add more schema parsing functions to `schema_utils`.


8.8.6
=====

* Added check for ES_HOST_LOCAL environment variable in ff_utils.get_es_metadata;
  for running Foursight checks locally (with local ssh tunnel to ES proxy);
  came up in foursight/checks/audit_checks (2024-04-23).
* Allow Python 3.12 (pyproject.toml).
* Added remove_empty_objects_from_lists options to structured_data.StructuredDataSet, defaulting
  to True, which deletes empty objects from lists; however, only from the *end* of a list; if
  this flag is True and there are non-empty objects following empty objects then we flag an error.
* Few general things initially related to and factored out of rclone support in smaht-submitr:
  - Added extract_file_from_zip to zip_utils.
  - Added http_utils with download function.
  - Added get_app_specific_directory, get_os_name, get_cpu_architecture_name, short_uuid to misc_utils.
  - Added are_files_equal, create_random_file to file_utils,  compute_file_md5, compute_file_etag,
    normalize_path, get_file_size, get_file_modified_datetime to file_utils.
  - Minor extra sanity check to search_for_file in file_utils.
  - Added deterministic ordering to paths returned by search_for_file in file_utils.
  - Added create_temporary_file_name and remove_temporary_file tmpfile_utils.
  - Minor fix to misc_utils.create_dict (do not create property only if its value is None).
  - Minor updates to utility dcicutils.scripts.view_portal_object.


8.8.5
=====

* Fix bug in `creds_utils` to register portal key managers instead of parent class


8.8.4
=====
* Minor fix in structured_data to not try to resolve empty refs in norefs mode;
  and added StructuredDataSet.unchecked_refs; not functionally substantive as
  used (only) with smaht-submitr/submit-metadata-bundle --info --refs.
* Added nrows and nsheets to data_reader; convenience for smaht-submitr/submit-metadata-bundle --info.
* Added test_progress_bar module for progress_bar testing; would like to add more tests.
* Fixed up captured_output module to handle UTF-8 encoding to help unit testing progress_bar.
* Added hooks to progress_bar to help unit testing.
* Added a find_nth_from_end and set_nth to misc_utils to help progress_bar unit testing.
* Added format_size and format_duration misc_utils; refactor from smaht-submitr.
* Added format_datetime, parse_datetime to datetime_utils; refactor from smaht-submitr; and some tests.
* Added check_only flag to portal_utils.Portal.{post,patch}_metadata (came up in ad hoc troubleshooting).


8.8.3
=====
* Minor fix in structured_data related to smaht-submitr progress monitoring.
* Added progress_bar module (orginally lived in smaht-submitr).
* Added Portal.is_schema_type_file to portal_utils.
* Updated deployment_utils.py with support for GOOGLE_API_KEY in smaht-portal;
  this is to get the version of the latest smaht-submitr metadata template;
  and also similarly for SUBMITR_METADATA_TEMPLATE_SHEET_ID.


8.8.2
=====
* Support for ExtraFiles pseudo-type, to handle extra_files in smaht-submitr..
* Minor structured_data fix related to counting unresolved references;
  not functionally consequential; only incorrect user feedback in smaht-submitr.
* Support in structured_data for norefs (completely ignore references).
* Minor fix in portal_object_utils.PortalObject._compare for lists.
* Minor structured_data changes for smaht-submitr validation/submission progress tracking.
* Minor structured_data code cleanup.
* Added submitr.progress_constants for sharing between smaht-submitr, snovault, smaht-portal;
  not ideal living here but driving us nuts maintaining in separate locations;
  and since we have this submitr sub-directory now, unified the common
  ref_lookup_strategy function from smaht-submitr and smaht-portal.


8.8.1
=====
* Changes to troubleshooting utility script view-portal-object.
* Some reworking of ref lookup in structured_data.
* Support ref caching in structured_data.
* Added hook to turn off ref lookup by subtypes in case we need this later.
* Added hook do ref lookup at root path first; set to true by smaht-portal for accession IDs.
* Moved/adapted test_structured_data.py from smaht-portal to here.


8.8.0
=====
* Changes to structured_data support date/time types.
* Changes to structured_data support internal references in any order.
* New datetime_utils module and tests; first created for date/time support in structured_data.
* Added view-portal-object script for general troubleshooting.
* Change to data_reader to ignore sheet names enclosed in parenthesis.


8.7.2
=====

* Changes to itemize SMaHT submission ingestion create/update/diff situation (portal_object_utils).
* Changes to structured_data to handle property deletes (portal_object_utils).


8.7.1
=====

* Changed scripts/publish_to_pypi.py to allow gitinfo.json to have unstaged changes;
  this is so we can optionally have repos write relevant git (repo, branch, commit) info
  to this file (via GitHub Actions) and make it accessible to the package for inspection.
* Added is_schema_type and is_specified_schema to portal_utils.Portal.
* Refactoring in portal_utils; added portal_object_utils; added file_utils.py.


8.7.0
=====

* Add new schema_utils module for schema parsing


8.6.0
=====

* Minor fix to misc_utils.to_integer to handle float strings.
* Minor fix to structured_data to accumulate unique resolved_refs across schemas.
* Added ability to autoadd properties structured_data.StructuredDataSet;
  to automatically pass in submission_centers on submission, and
  not require that the user explicitly set this in the spreadsheet.
* Changes to structured_data to respect uniqueItems for arrays.
* Handle no schemas better in structured_data.
* Added portal_utils.Portal.ping().
* Minor fix in portal_utils.Portal._uri().


8.5.0
=====

* Moved structured_data.py from smaht-portal to here; new portal_utils and data_readers modules.
* Strip sheet name in data_readers.Excel; respecte (ignore) hidden sheets.


8.4.0
=====

* More work related to SMaHT ingestion (bundle/sheet_utils, data_readers, etc).


8.3.0
=====

* Updates for RAS to Redis API

8.2.0
=====

* 2023-11-02
* Added ``SchemaManager.get_identifying_properties`` in ``bundle_utils``
  which implicitly adds ``identifier`` to ``identifyingProperties``.
* Added support for ``portal_vapp`` to to `ff_utils.get_metadata``.


8.1.0
=====

* New module ``bundle_utils.py`` that is intended for schema-respecting worksheets ("metadata bundle").
  There are various modular bits of functionality here, but the main entry point here is:

  * ``load_items`` to load data from a given table set, doing certain notational canonicalizations, and
    checking that things are in the appropriate format.

* In ``common.py``, new hint types:

  * ``CsvReader``
  * ``JsonSchema``
  * ``Regexp``

* In ``lang_utils.py``:

  * New arguments ``just_are=`` to ``there_are`` get verb conjugation without the details.

  * Add "while" to "which" and "that" as clause handlers in the string pluralizer
    (e.g., so that "error while parsing x" pluralizes as "errors while parsing x")

  * ``conjoin_list`` and ``disjoin_list`` now call ``str`` on their sequence elements so that things like
    ``conjoined_list([2, 3, 4])`` are possible.

* In ``misc_utils.py``, miscellaneous new functionality:

  * New class ``AbstractVirtualApp`` that is either an actual VirtualApp or can be used to make mocks
    if the thing being called expects an ``AbstractVirtualApp`` instead of a ``VirtualApp``.

  * New function ``to_snake_case`` that assumes its argument is either a CamelCase string or snake_case string
    and returns the snake_case form.

  * New function ``is_uuid`` (migrated from Fourfront)

  * New function ``pad_to``

  * New class ``JsonLinesReader``

* In ``qa_checkers.py``:

  * Change the ``VERSION_IS_BETA_PATTERN`` to recognize alpha or beta patterns. Probably a rename would be better,
    but also incompatible. As far as I know, this is used only to not fuss if you haven't made a changelog entry
    for a beta (or now also alpha).

* New module ``sheet_utils.py`` for loading workbooks in a variety of formats, but without schema interpretation.

  A lot of this is implementation classes for each of the kinds of files, but the main entry point
  is intended to be ``load_table_set`` if you are not working with schemas. For schema-related support,
  see ``bundle_utils.py``.

* New module ``validation_utils.py`` with these facilities:

  * New class ``SchemaManager`` for managing a set of schemas so that programs asking for a schema by name
    only download one time and then use a cache. There are also facilities here for populating a dictionary
    with all schemas in a table set (the kind of thing returned by ``load_table_set`` in ``sheet_utils.py``)
    in order to pre-process it as a metadata bundle for checking purposes.

  * New functions:

    * ``validate_data_against_schemas`` to validate that table sets (workbooks, or the equivalent) have rows
      in each tab conforming to the schema for that tab.

    * ``summary_of_data_validation_errors`` to summarize the errors obtained from ``validate_data_against_schemas``.


8.0.0
=====

* Update Python to 3.11; and nixed Python 3.7.
* Updated boto3/botocore versions.
* Updatad pyyaml version to ^6.0.1; Mac M1 has issues building 5.4.1 (though 5.3.1 works).
  See PyYAML 6.0 change log here: https://github.com/yaml/pyyaml/blob/master/CHANGES
  The only incompatible change seems to be that yaml.load now requires a Loader argument;
  and searching our GitHub organizations (4dn-dcic, dbmi-bgm, smaht-dac) the only ones which might
  be affected are cwltools and parliament2, neither of which are dependent on dcicutils in any way.


7.13.0
======

* In ``license_utils``:

  * Add an ``RLanguageFramework``.

  * Add various additional checker classes, and a registry to catalog them. Refactor so that pre-existing
    classes better share information in an inherited way.

    +------------------------------------------+--------------------------------+----------------+
    |                 Class                    |          Checker Name          |    Status      |
    +==========================================+================================+================+
    | ``ParkLabCommonLicenseChecker``          | ``park-lab-common``            | New            |
    +------------------------------------------+--------------------------------+----------------+
    | ``ParkLabGplPipelineLicenseChecker``     | ``park-lab-gpl-pipeline``      | New            |
    +------------------------------------------+--------------------------------+----------------+
    | ``ParkLabCommonServerLicenseChecker``    | ``park-lab-common-server``     | New            |
    +------------------------------------------+--------------------------------+----------------+
    | ``C4InfrastructureLicenseChecker``       | ``c4-infastructure``           | Refactored     |
    +------------------------------------------+--------------------------------+----------------+
    | ``C4PythonInfrastructureLicenseChecker`` | ``c4-python-infrastructure``   | Refactored     |
    +------------------------------------------+--------------------------------+----------------+
    | ``Scan2PipelineLicenseChecker``          | ``scan2-pipeline``             | New            |
    +------------------------------------------+--------------------------------+----------------+

* In ``misc_utils``:

  * New function ``json_file_contents``

* In ``scripts``:

  * Add a ``run-license-checker`` script, implemented by ``run_license_checker.py``,
    that runs the license checker whose "checker name" is given as an argument.


7.12.0
======

* In ``glacier_utils``:

  * Add functionality for KMS key encrypted accounts


7.11.0
======

* In ``ff_utils``:

  * Fix in ``get_schema`` and ``get_schemas`` for the ``portal_vapp`` case needing a leading slash on the URL.
  * Fix in ``get_schema`` and ``get_schemas`` for the ``portal_vapp`` returning webtest.response.TestResponse
    which has a ``json`` object property rather than a function.


7.10.0
======

* In ``ff_utils``:

  * New arguments ``portal_env=`` and ``portal_vapp`` to ``get_schema``
    for function ``get_schema`` and ``get_schemas``.

* In ``s3_utils``:

  * Fix a failing test (caused by an environmental change, no functional change).

* In ``license_utils``:

  * Allow C4 infrastructure to use the ``chardet`` library.


7.9.0
=====

* In ``misc_utils``:

  * New function ``to_camelcase`` that can take either snake_case or CamelCase input.

* In ``qa_utils``:

  * New function ``is_subdict`` for asymmetric testing of dictionary equivalence.

* In ``ff_utils``:

  * New function ``get_schema`` that will pull down an individual schema definition.
  * New function ``get_schemas`` that will pull down all schema definitions.
  * New argument ``allow_abstract`` to ``get_schema_names``
    for conceptual compatibility with ``get_schemas``.
  * Minor tweaks to ``dump_results_to_json`` for style reasons,
    and repairs to its overly complex and error-prone unit test.


7.8.0
=====

* Add ``variant_utils`` with tools to filter through CGAP data.


7.7.2
=====

* In ``license_utils``:

  * In ``license_utils.C4InfrastructureLicenseChecker``, allow exceptions for
    libraries ``dnslib``, ``dnspython``, ``node-forge`` and ``udn-browser``.


7.7.1
=====

* Fix tests are failing on utils master branch (`C4-1081 <https://hms-dbmi.atlassian.net/browse/C4-1081>`_), a problem with the ``project_utils`` test named ``test_project_registry_make_project_autoload``.


7.7.0
=====

* Add ``license_utils`` with tools to check license utilities.

  .. note::

     Using these utilities requires you to have a dev dependency on ``pip-licenses``.
     If it's not there, you'll get an error telling you this fact.

     Effectively, though, we're exporting a required dev dependency, since we did not
     want to make this a runtime dependency.

     (You can also attend to this dependency by arranging to ``pip install pip-licenses``
     before running tests.)

* Add ``contribution_utils`` with tools to track repository contributions.


7.6.0
=====

* In ``creds_utils``:

  * Support for ``SMaHTKeyManager``


7.5.3
=====

* EnvUtils updates to accommodate ``smaht-portal``


7.5.2
=====

* Add deployer class for ``smaht-portal``


7.5.1
=====
* In ``scripts/publish_to_pypi`` default to not allowing publish using (non-API-token) username,
  and fixed package name to come from pyproject.toml rather than git repo name (used only for
  display purposes and checking if version already pushed).


7.5.0
=====

* In ``lang_utils``:

  * Teach ``EnglishUtils.string_pluralize`` about words ending in ``-ses`` because ``cgap-portal`` needs this.

* New module ``project_utils`` with support for Project mechanism.

  * New decorators ``ProjectRegistry`` and ``C4ProjectRegistry``

  * New class ``Project`` and ``C4Project``

* In ``qa_utils``:

  * In class ``MockFileSystem``:

    * New method ``abspath``
    * New method ``chdir``
    * New method ``expanduser``
    * New method ``getcwd``
    * New method ``mock_exists_open_remove_abspath_getcwd_chdir`` (context manager)


7.4.4
=====

Fixed the ``publish-to-pypi`` script to ignore the ``.gitignore`` file when looking for untracked files.


7.4.3
=====

Removed ``scripts`` from ``packages`` directory list in ``pyproject.toml``; not necessary.


7.4.2
=====

* Rewrite test ``test_get_response_json`` as a unit test to get around its flakiness.


7.4.1.1
=======

The ``glacier2`` branch did not bump the version. It continues to call itself version 7.4.1 even though the ``v7.4.1`` does not contain its functionality, so the point of change is retroactiely tagged ``v7.4.1.1``.

* In ``common.py``

  * Add constant ``ENCODED_LIFECYCLE_TAG_KEY``

* In ``glacier_utils.py``:

  * Accept support for url-encoded tags for GlacierUtils multipart uploads.

  * Add support for removing lifecycle tag when copying object.


7.4.1
=====

* In ``glacier_utils.py``:

  * Fix calls to ``self.copy_object_back_to_original_location``
    in ``restore_glacier_phase_two_copy``.

* In ``qa_utils.py``:

  * Make ``boto3.client('s3').put_object`` handle either a string
    or bytes object correctly.

* Actively mark tests that are already marked with
  ``pytest.mark.beanstalk_failure`` to also use ``pytest.mark.skip``
  so they don't run and confuse things even when markers are not in play.

* Update some live ecosystem expectations to match present real world state.

* Separate tests of live ecosystem so that the parts that are supposed
  to pass reliably are in a separate function from the parts that are
  thought to be in legit transition.

* Misc changes to satisfy various syntax checkers.

  * One stray call to `print` changed to `PRINT`.

  * Various grammar errors fixed in comment strings because
    PyCharm now whines about that, and the suggestions seemed reasonable.


7.4.0
=====

* In ``dcicutils.env_utils`` added function ``get_portal_url`` which is
  the same as ``get_env_real_url`` but does not access the URL (via the
  health page); first usage of which was in foursight-core. 2023-04-16.

* Added ``dcicutils.ssl_certificate_utils``;
  first usage of which was in foursight-core. 2023-04-16.

* Added ``dcicutils.scripts.publish_to_pypi``; 2023-04-24.

* Added ``dcicutils.function_cache_decorator``; 2023-04-24;
  future help in simplifying some caching in foursight-core APIs.

* Updated ``test/test_task_utils.py`` (``test_pmap_parallelism``):
  to increase ``margin_of_error`` to 1.1333.


7.3.1
=====

Add LICENSE.txt (MIT Licenses). The ``pyproject.toml`` already declared that lic
ense, so no real change. Just pro forma.


7.3.0
=====

* In ``dcicutils.command_utils``:

  * New decorator ``require_confirmation``

* In ``dcicutils.common``:

  * New variable ``ALL_S3_STORAGE_CLASSES``
  * New variable ``AVAILABLE_S3_STORAGE_CLASSES``
  * New variable ``S3_GLACIER_CLASSES``
  * New type hint ``S3GlacierClass``
  * New type hint ``S3StorageClass``

* New module ``dcicutils.glacier_utils``:

    * Class for interacting with/restoring files from Glacier

* In ``dcicutils.misc_utils``:

  * New function ``INPUT``
  * New function ``future_datetime``
  * New decorator ``managed_property``
  * New function ``map_chunked``
  * New function ``format_in_radix``
  * New function ``parse_in_radix``

* In ``dcicutils.qa_checkers``:

  * Fix bug in ``print`` statement recognizer

* In ``dcicutils.qa_utils``:

  * Support for Glacier-related operations in ``MockBotoS3Client``:

    * Method ``copy_object``
    * Method ``delete_object``
    * Method ``list_object_versions``
    * Method ``restore_object``

* Load ``coveralls`` dependency only dynamically in GA workflow, not in poetry,
  because it implicates ``docopt`` library, which needs ``2to3``, and would fail.



7.2.0
=====

* In ``exceptions``:

  * New class ``MultiError``

* In ``qa_utils``:

  * New class ``Timer``

* In ``misc_utils``:

  * New generator function ``chunked``

* New module ``task_utils``:

  * New class ``Task``
  * New class ``TaskManager``
  * New function ``pmap``
  * New function ``pmap_list``
  * New function ``pmap_chunked``

* Adjust expectations for environment ``hotseat``
  in live ecosystem integration testing by ``tests/test_s3_utils.py``


7.1.0
=====

* New ``trace_utils`` module

  * New decorator ``@Trace``

  * New function ``make_trace_decorator`` to make similar ones.

* Fix to ``obfuscation_utils`` relating to dicts containing lists.

* In ``dcicutils.misc_utils``:

  * New function ``deduplicate_list``

* In ``dcicutils.qa_utils``:

  * Fixes to the ``printed_output`` context manager relating to multi-line ``PRINT`` statements.


7.0.0
=====

* New files: ``dcicutils.redis_utils`` and ``dcicutils.redis_tools`` plus associated test files

* In ``dcicutils.redis_utils``:

  * Implement the ``RedisBase`` object, which takes the output of ``create_redis_client`` and returns
    an object that implements some base APIs for interacting with Redis.

* In ``dcicutils.redis_tools``:

  * Implement the ``RedisSessionToken`` object, which creates higher level APIs for creating session
    tokens that are backed by Redis. This object operates on the ``RedisBase`` class.
  * Session tokens are 32 bytes and expire automatically after 3 hours by default, but can be tuned
    otherwise.

* In ``dcicutils.command_utils``:

  * Make ``script_catch_errors`` context manager return a ``fail``
    function that can be called to bypass the warning that an error
    needs to be reported.

* In ``dcicutils.common``:

  * Add a number of type hints.

* In ``dcicutils.ff_utils``:

  * Refactor ``unified_authentication`` to be object-oriented.

  * Add some type hinting.

* In ``dcicutisl.env_base`` and ``dcicutils.s3_utils``:

  * Add some error checks if stored s3 credentials are not in the right form. (**BREAKING CHANGE**)
    This is not expected to break anything, but users should be on the lookout for problems.

  * Add a new argument (``require_key=``, default ``True``) to ``s3Utils.get_access_keys()`` so that checking
    of the key name can be relaxed if only ``secret`` and ``server`` are needed, as might happen for Jupyterhub creds.
    This is a possible way of addressing unexpected problems that could come up due to added error checks.

  * Add some type hinting.

  * Add comments about other possible future error checking.

* In ``dcicutils.misc_utils``:

  * New function ``utc_now_str``

* Misc PEP8


6.10.1
======

* Various test adjustments to accommodate health page changes related to
  `C4-853 <https://hms-dbmi.atlassian.net/browse/C4-853>`_.


6.10.0
=====

* Move ``mocked_s3utils_with_sse`` from ``test_ff_utils.py`` to ``ff_mocks.py``.


6.9.0
=====

* In ``dcicutils.misc_utils``:

  * Add method ``is_c4_arn`` to check if given ARN looks like CGAP or Fourfront entity.


6.8.0
=====

* In ``dcicutils.deployment_utils``:

  * Add support for ``Auth0Domain`` and ``Auth0AllowedConnections``


6.7.0
=====

* In ``dcicutils.qa_utils``:

  * For method ``Eventually_call_assertion``:

    * Make the ``error_message=`` argument actually work.

    * The ``threshold_seconds=`` argument is now deprecated.
      Please prefer ``tries=`` and/or ``wait_seconds=``.

    * Fix a bug where it didn't wait between iterations.

  * Add a method ``consistent`` that is a class method / decorator (named ``Eventually.consistent``).

  * Add testing, particularly of the timing.

* In ``dcicutils.cloudformation_utils``:

  * When searching for checkrunners, be more forgiving about abbreviations for development (dev)
    and production (prd, prod).


6.6.0
=====

* In ``dcicutils.misc_utils``:

  * Add ``keys_and_values_to_dict`` function (and associated unit test).


6.5.0
=====

* In ``dcicutils.qa_utils``:

  * Add ``Eventually.call_assertion``.


6.4.1
=====
* Minor fix to ``obfuscate_dict`` in ``obfuscation_utils`` to respect passed ``obfuscated`` argument recursively.


6.4.0
=====

* In ``misc_utils``:

  * New class ``TopologicalSorter`` for topological sorting of graphs


6.3.1
=====

* New function ``env_equals`` in ``env_utils`` module.


6.3.0
=====

* Add ``opensearch_utils``, a forward-compatible OpenSearch client we should migrate to over time

* In ``codebuild_utils``:

  * New method ``run_project_build_with_overrides`` to allow running builds changing the build branch and environment variables


6.2.0
=====

* In ``lang_utils``:

  * New method EnglishUtils.parse_relative_time_string

* In ``misc_utils``:

  * New function ``str_to_bool``


6.1.0
=====

* In ``misc_utils``:

  * New decorator ``@classproperty``

  * New decorator ``@classproperty_cached``

  * New decorator ``@classproperty_cached_each_subclass``

  * New class ``Singleton``. Users of ``SingletonManager`` might prefer this,
    but we'll continue to support both. (No deprecation for now.)

  * In function ``is_valid_absolute_uri``, better handling of argument type errors.

  * For ``CachedField``:

    * Added a handler for ``__str__`` that returns useful information, which can also be used for ``__repr__``.

    * Fixed handler for ``__repr__`` to return a properly executable expression (shared with ``__str__``).

  * Improved test coverage by adding tests for some parts of the code that were not previously tested.

* In ``qa_utils``:

  * New class ``MockId`` for mocking the ``id`` function in a predictable way.

  * Adjust ``MOCK_QUEUE_URL_PREFIX`` to use a mocked URL that looks more
    like modern AWS url, where ``queue.amazonaws.com`` has been replaced by
    ``sqs.us-east-1.amazonaws.com``.


6.0.0
=====

`PR 224: ElasticSearch 7 <https://github.com/4dn-dcic/utils/pull/224>`_

* Updates ElasticSearch to version 7.13.4, the highest version we can tolerate
  of this library. This utils version is a requirement for using ES7 or
  OpenSearch 1.3 in production.


5.3.0
=====

`PR 223: Refactored recording tech <https://github.com/4dn-dcic/utils/pull/223>`_

* Refactor ``TestRecorder`` into an ``AbstractTestRecorder`` with two concrete classes,
  ``RequestsTestRecorder`` and ``AuthorizedRequestsTestRecorder``. The new refactor means
  it'll be easier to write other subclasses.

  The new classes take their arguments slightly differently, but all test cases are updated,
  and this was previously broken in (so not used in) other repositories and it can't break
  anything elsewhere to change the conventions. We're treating this as a simple bug fix.

* Deprecated unused class ``MockBoto4DNLegacyElasticBeanstalkClient``.


5.2.1
=====

`PR 222: Improved IntegratedFixture and static check cleanups <https://github.com/4dn-dcic/utils/pull/222>`_

* Show fewer uninteresting tracebacks on static test failures.

* Small incompatible changes to recently released qa-related items:

  * In ``qa_checkers.confirm_no_uses``, remove the new ``if_used`` argument in favor of a simpler implementation.

  * Slightly rerefactored the class hierarchy so that ``StaticChecker`` is a smaller class that doesn't have quite
    as much functionality, and ``StaticSourcesChecker`` corresponds to what ``StaticChecker`` previously did.

  Since this is all testing-only, not something used in production, and since there are believed to not yet be uses
  outside the repo, we're treating this as a bug fix (patch version bump) not an incompatible change (which would
  entail a major version bump and a lot of fussing for nothing).

* Make class initialization of ``IntegratedFixture`` happen at instance-creation time.
  That simplifies the loading actions needed. Those can happen in ``conftest.py`` rather than in
  ``dcicutils.ff_mocks``, which in turn should allow ``dcicutils.ff_mocks`` to be imported without error,
  fixing `C4-932 <https://hms-dbmi.atlassian.net/browse/C4-932>`_


5.2.0
=====

* Some functionality moved from ``qa_utils`` to ``qa_checkers``.
  In each case, to be compatible, the ``qa_utils`` module will continue
  to have the entity availble for import until the next major release.

  * Class ``VersionChecker``
  * Class ``ChangeLogChecker``
  * Function ``confirm_no_uses``
  * Function ``find_uses``
  * Variable ``QA_EXCEPTION_PATTERN``

  As an official matter, use of these moved entities from by importing
  them from ``dcicutils.qa_utils`` is deprecated. Please update programs
  to import these from ``dcicutils.qa_checkers`` instead.

* New functionality in ``qa_checkers``:

  * New class ``DocsChecker``
  * New class ``DebuggingArtifactChecker``

* In ``misc_utils``:

  * New function ``lines_printed_to``.

* New ``pytest`` marker ``static`` for static tests.

* New ``make`` target ``test-static`` to run tests marked with
  ``@pytest.mark.static``.

* New GithubActions (GA) workflow: ``static_checks.yml``


5.1.0
=====

* In ``qa_utils``:

  * New class ChangeLogChecker, like VersionChecker, but it raises an error
    if there's a change log inconsistency.


5.0.0
=====

* Drop support for Python 3.6 (**BREAKING CHANGE**)


4.8.0
=====

* New functionallity in ``ecr_utils.ECRUtils`` in support of planned changes to Foursight:

  * Add ``ECRTagWatcher`` class that can be used to watch for a new image with a given tag in an ECS repository.

* New functionality in ``qa_utils`` to support a mock ECR client.

* Refactor parts of ``ecr_utils`` and ``ecr_scripts`` to move some general-purpose parts out of
  ``ecr_scripts`` (top-level variables and class ``ECRCommandContext``)
  and into ``ecr_utils`` (class ``ECRUtils``):

  * Changes to arguments for ``ECRUtils`` constructor:

    * Allow additional arguments needed for moved methods.
    * Default more arguments so that only relevant ones need be passed.

  * Move some methods from ``ECRCommandContext`` to ``ECRUtils``:

    * ``get_images_descriptions``
    * ``_apply_image_descriptions_limit``

  * Certain variables at ``ecr_scripts`` top-level became class variables in ``ecr_utils.ECRUtils``
    (some with some renaming):


    +------------------------+------------------------+--------------------------------+------------------------+
    | .. raw:: html                                   | .. raw:: html                                           |
    |                                                 |                                                         |
    |    <center><tt>ecr_scripts</code></tt>          |    <center><tt>ecr_utils.ECRUtils</tt></center>         |
    |                                                 |                                                         |
    +------------------------+------------------------+--------------------------------+------------------------+
    | module variable        | module variable status | class variable                 | class variable status  |
    +========================+========================+================================+========================+
    | DEFAULT_ECS_REPOSITORY | deprecated             | DEFAULT_IMAGE_REPOSITORY       | new                    |
    +------------------------+------------------------+--------------------------------+------------------------+
    |  IMAGE_COUNT_LIMIT     | deprecated             | IMAGE_LIST_DEFAULT_COUNT_LIMIT | new                    |
    +------------------------+------------------------+--------------------------------+------------------------+
    | IMAGE_LIST_CHUNK_SIZE  | deprecated             | IMAGE_LIST_CHUNK_SIZE          | new                    |
    +------------------------+------------------------+--------------------------------+------------------------+
    | RELEASED_TAG           | deprecated             | IMAGE_RELEASED_TAG             | new                    |
    +------------------------+------------------------+--------------------------------+------------------------+

* Unit tests for new functionality, and backfilled unit tests for some parts of ``ecr_utils``.


4.7.0
=====

* In ``env_utils``:

  * New function ``foursight_env_name``, an alias for
    ``lambda envname: infer_foursight_from_env(envname=envname)``

* Add error checking for running tests that looks to see that we're in the right account before we move ahead
  only to find this out in a less intelligible way.


4.6.0
=====

* In ``env_utils``:

  * Add ``identity_name`` arguments to:

    * ``apply_identity``
    * ``assumed_identity_if``
    * ``assumed_identity``
    * ``get_identity_secrets``

  * Remove buggy defaulting of value for ``get_identity_name``.
  * Improve error messages in ``get_identity_secrets``.


4.5.0
=====

* A few other changes to ``lang_utils.string_pluralize`` to give more refined
  control of punctuation and to allow phrases with "that is/was" or
  "which is/was" qualifiers.


4.4.1
=====

* In ``ff_utils``;

  * add function ``get_search_facet values`` to support count from facets from any search


4.4.0
=====

* In ``lang_utils``:

  * Add ```"from"`` and ``"between"`` to the list of prepositions that the pluralizer understands.

* In ``obfuscation_utils``:

  * Add ``is_obfuscated`` to predicate whether something is in obfuscated
    form. Among other things, this enables better testing.

  * Add an ``obfuscated=`` argument to ``obfuscate`` and ``obfuscate_dict``,
    allowing the choice of what obfuscated value to use. The argument must
    be something for which ``is_obfuscated`` returns True.

NOTE: Due to a versioning error in beta, there was no 4.3.0. The previous released version was 4.2.0.


4.2.0
=====

* In ``command_utils``:

  * Add ``script_catch_errors`` context manager, borrowed from ``SubmitCGAP``.

* In ``ff_utils``:

  * Add ``is_bodyless`` predicate on http methods (verbs) to say if they want a data arg.

* In ``env_base``:

  * Add ``EnvBase.set_global_env_bucket`` to avoid setting ``os.environ['GLOBAL_ENV_BUCKET']`` directly.


4.1.0
=====

* Add better ``CHANGELOG.rst`` for the changes that happened in 4.0.0.
* Add unit testing for stray ``print(...)`` or ``pdb.set_trace()``
* Support for ``ENCODED_CREATE_MAPPING_SKIP``, ``ENCODED_CREATE_MAPPING_WIPE_ES``,
  and ``ENCODED_CREATE_MAPPING_STRICT`` in GAC to allow ``$CREATE_MAPPING_SKIP,``
  ``$CREATE_MAPPING_WIPE_ES``, and ``$CREATE_MAPPING_STRICT`` in ``.ini`` files.
* Allow ``get_foursight_bucket`` to infer a bucket prefix if one is not
  explicitly supplied. (The heuristic removes ``-envs`` from the global env bucket
  name and uses what remains.)
* Fix test recording capability. Add (though unused) ability to record at
  the abstraction level of ``authorized_request``.
* Fix various tests that had grown stale due to data changes.

  * ``test_post_delete_purge_links_metadata`` (needed to be re-recorded)
  * ``test_upsert_metadata`` (needed to be re-recorded)
  * ``test_unified_authentication_prod_envs_integrated_only``
    (simplified, removed bogus attempts at recording)
  * ``test_faceted_search_exp_set`` (needed many different counts)
  * ``test_some_decorated_methods_work`` (needed one different count)
  * ``test_faceted_search_exp_set`` (newly recorded)
  * ``test_faceted_search_users`` (newly recorded)

* Specify pytest options in pyproject.toml instead of a separate file.
* In ``env_utils``:

  * Added ``EnvUtils.app_name`` to get the orchestrated app name.
  * Added ``EnvUtils.app_case`` to conditionalize on ``if_cgap=`` and ``if_fourfront=``.

* In ``qa_utils``:

  * Added an ``input_mocked`` context manager.
  * Added ``MockLog`` and a ``logged_messages`` context manager.


4.0.2
=====

* In ``cloudformation_utils``:

  * New function ``find_lambda_function_names`` in ``AbstractOrchestrationManager`` which
    factors out the lookup part from the ``discover_foursight_check_runner_name`` function.

* In ``obfuscation_utils``:

  * Changed ``should_obfuscate`` to include "session" related keys.


4.0.1
=====
* In ``qa_utils``:

  * New class ``MockBoto3Ec2`` geared toward security group rules related unit testing.

* New ``obfuscation_utils`` module.


4.0.0
=====

The following change list is only interim. A followup change will revise this entry with better information
covering what changed in 4.0, which is considerably more.

* Some new modules. The scripts modules came from other repositories, for centralization reasons. The other modules
  are originally refactorings to make functionality more broadly available at various stages of bootstrapping
  this library.

  * ``ecr_scripts`` has support for command line scripts related to ECR repositories.
  * ``env_base`` has support for bits of environmental foothold needed before ``env_utils`` or ``s3_utils`` are ready.
  * ``env_manager`` is a higher-level environmental abstraction built after ``env_utils`` is available.
  * ``env_scripts`` has support for command line scripts related to configurable environments and the global env bucket.

* New ``make`` targets:

  * ``make test-all`` runs all tests
  * ``make test-most`` runs all unit and integration tests (marked ``unit``, ``integration`` or ``integrationx``),
    but not things likely to fail (marked ``beanstalk failure`` or ``direct_es_query``).
  * ``make test-integrations`` runs all integration tests (marked ``integration`` or ``integrationx``),
    but not things likely to fail (marked ``beanstalk failure`` or ``direct_es_query``).
  * ``make test-direct-es-query`` runs any test marked ``direct_es_query```.
  * ``test-units-with-coverage`` runs unit tests with the ``coverage`` feature.
  * ``test-for-ga`` is an indirect way to call ``test-units-with-coverage``, and will be what the GithubActions
    workflow calls.

* Configurable environmental support for orchestrated C4 applications (Fourfront and CGAP) in ``env_utils``
  (`C4-689 <https://hms-dbmi.atlassian.net/browse/C4-689>`_).

* Extend that support to allow mirroring to be enabled
  (`C4-734 <https://hms-dbmi.atlassian.net/browse/C4-734>`_).

The net result is a configurable environment in which the env descriptor in the global env bucket can contain
these new items:

===============================  ===============================================================================
    Key                              Notes
===============================  ===============================================================================
``"dev_data_set_table"``         Dictionary mapping envnames to their preferred data set
``"dev_env_domain_suffix"``      e.g., .abc123def456ghi789.us-east-1.rds.amazonaws.com
``"foursight_bucket_table"``     A table mapping environments to another table mapping chalice stages to buckets
``"foursight_url_prefix"``       A prefix string for use by foursight.
``"full_env_prefix"``            A string like "cgap-" that precedes all env names
``"hotseat_envs"``               A list of environments that are for testing with hot data
``"indexer_env_name"``           The environment name used for indexing (being phased out)
``"is_legacy"``                  Should be ``"true"`` if legacy effect is desired, otherwise omitted.
``"stage_mirroring_enabled"``    Should be ``"true"`` if mirroring is desired, otherwise omitted.
``"orchestrated_app"``           This allows us to tell 'cgap' from 'fourfront', in case there ever is one.
``"prd_env_name"``               The name of the prod env
``"public_url_table"``           Dictionary mapping envnames & pseudo_envnames to public urls
``"stg_env_name"``               The name of the stage env (or None)
``"test_envs"``                  A list of environments that are for testing
``"webprod_pseudo_env"``         The pseudo-env that is a token name to use in place of the prd env for shared
                                 stg/prd situations, replacing ``fourfront-webprod`` in the legacy system.
                                 (In orchestrations, this should usually be the same as the ``prd_env_name``.
                                 It may or may not need to be different if we orchestrate the legacy system.)
===============================  ===============================================================================

* In ``base``:

  * ``compute_prd_env_for_project``
  * ``compute_stg_env_for_project``
  * ``get_env_info`` (replaces ``beanstalk_utils.get_beanstalk_info``)
  * ``get_env_real_url`` (replaces ``beanstalk_utils.get_beanstalk_real_url``)

* In ``beanstalk_utils``:

  * Removed:

    * ``swap_cname``

    NOTE: This was never invoked by automatic programs, so we didn't do a deprecation stage.

  * Deprecated:

    * ``get_beanstalk_info`` is deprecated. Use ``beanstalk_utils.get_env_info``.
    * ``get_beanstalk_real_url`` is deprecated. Use ``env_utils.get_env_real_url``.

    NOTE: These continue to work for now, but will be removed in the future.
    Please update code to use recommended replacement.

* In ``cloudformation_utils``:

  * Added function``discover_foursight_check_runner_name``.
  * Added function ``tokenify``.
  * Moved ``DEFAULT_ECOSYSTEM`` to ``cloudformation_utils``. Importing it from this library is now deprecated.

* In ``common``:

  * New variables:

    * ``CHALICE_STAGE_DEV``
    * ``CHALICE_STAGE_PROD``
    * ``CHALICE_STAGES``
    * ``DEFAULT_ECOSYSTEM`` (moved from ``cloudformation_utils``)
    * ``LEGACY_CGAP_GLOBAL_ENV_BUCKET``
    * ``LEGACY_GLOBAL_ENV_BUCKET``

  * New type hint (variable):

    * ``ChaliceStage``

* In ``ecr_utils``:

  * Removed ``CGAP_ECR_LAYOUT``.  Use ``ECRUtils.ECR_LAYOUT`` instead.
  * Deprecated ``CGAP_ECR_REGION``. Use ``ECRUtils.REGION`` or ``common.REGION`` instead.

* In ``ecs_utils``:

  * Added ``ECSUtils.REGION``.

* In ``env_base``:

  * Moved ``EnvBase`` to here from ``s3_utils``.
  * Added ``s3_utils.s3Base`` (factored out of ``s3_utils.s3Utils``)

* In ``env_utils``:

  * Removed:

    * ``guess_mirror_env``
    * ``make_env_name_cfn_compatible``

    NOTE: This was not believed to be used anywhere so is presumably no great hardship.
    (Kent also didn't like the naming, which used a confusing abbreviation.)

  * New functions:

    * ``blue_green_mirror_env``
    * ``compute_prd_env_for_project``
    * ``data_set_for_env``
    * ``ecr_repository_for_env``
    * ``full_cgap_env_name``
    * ``full_fourfront_env_name``
    * ``get_env_from_context``
    * ``get_env_real_url`` (replaces ``beanstalk_utils.get_beanstalk_real_url``)
    * ``get_foursight_bucket``
    * ``get_foursight_bucket_prefix``
    * ``get_standard_mirror_env``
    * ``has_declared_stg_env``
    * ``indexer_env_for_env`` (introduced _and_ deprecated during beta)
    * ``infer_foursight_from_env``
    * ``infer_foursight_url_from_env``
    * ``is_indexer_env`` (introduced _and_ deprecated during beta)
    * ``is_orchestrated``
    * ``maybe_get_declared_prd_env_name``
    * ``permit_load_data``

  * New classes:

    * ``ClassificationParts``
    * ``EnvNames``
    * ``EnvUtils``
    * ``PublicUrlParts``

  * Always erring:

    * ``indexer_env_for_env``
    * ``is_indexer_env``

    NOTE: These functions unconditionally raise an error indicating that the functionality is no longer available.
          Their callers must be rewritten, probably in a way that is not a simple substitution.

  * Removed all top-level variables from ``env_utils`` variables, moving them to ``env_utils_legacy``.
    This includes but is not limited to variables with names starting with ``CGAP_``, ``FF_`` or ``BEANSTALK_``.
    These are deprecated and should not be used outside of ``dcicutils``.
    Within ``dcicutils``, they may be used only for testing.
    All ``env_utils`` functionality should be accessed through functions, not variables.

* In ``exceptions``:

  * ``BeanstalkOperationNotImplemented``
  * ``EnvUtilsLoadError``
  * ``IncompleteFoursightBucketTable``
  * ``LegacyDispatchDisabled``
  * ``MissingFoursightBucketTable``
  * ``NotUsingBeanstalksAnyMore``

* Added tech debt by disabling certain tests or marking them for later scrutiny.

  Three new pytest markers were added in ``pytest.ini``:

  * ``beanstalk_failure`` - An obsolete beanstalk-related test that needs fixing
  * ``direct_es_query`` - A test of direct ES _search that is disabled for now
    and needs to move inside the firewall
  * ``stg_or_prd_testing_needs_repair`` - Some or all of a test that was failing on stg/prd
    has been temporarily disabled
  * ``recordable`` declares a test to use "recorded" technology so that if ``RECORDING_ENABLED=TRUE``,
    a new test recording is made


3.16.0
======

* In ``qa_utils``:

  * Extend the mocking so that output to files by ``PRINT`` can be tested
    by ``with printed_output as printed`` using ``printed.file_last[fp]``
    and ``printed.file_lines[fp]``.


3.15.0
======

* In ``ecs_utils``:
  * Adds the ``service_has_active_deployment`` method.


3.14.2
======
* In ``qa_utils``:
  * Minor updates related PEP8.


3.14.1
======
* In ``qa_utils``:

  * New class ``MockBotoS3Iam``.
  * New class ``MockBotoS3Kms``.
  * New class ``MockBotoS3OpenSearch``.
  * New class ``MockBotoS3Sts``.
  * New method  ``MockBotoS3Session.get_credentials``.
  * New method ``MockBotoS3Session.put_credentials_for_testing``.
  * New property ``MockBotoS3Session.region_name``.
  * New method ``MockBotoS3Session.unset_environ_credentials_for_testing``.


3.14.0
======

* In ``misc_utils``:

  * New function ``key_value_dict``.
  * New function ``merge_key_value_dict_lists``.

* In ``qa_utils``:

  * Add ``MockBotoS3Client.get_object_tagging``.
  * Add ``MockBotoS3Client.put_object_tagging``.

* In ``s3_utils``:

  * Add ``s3Utils.get_object_tags``
  * Add ``s3Utils.set_object_tags``
  * Add ``s3Utils.set_object_tag``


3.13.1
======

* Fix a bug in ``diff_utils``.


3.13.0
======

* In ``deployment_utils``:

  * In ``CreateMappingOnDeployManager``:

    * Add ``get_deploy_config`` with slightly different arguments than ``get_deployment_config``,
      so CGAP and FF can be more easily compared.

    * Change ``--strict`` and ``--skip`` to not take an argument on the command line, and to default to False.

      NOTE: After some discussion, this was decided to be treated as a bug fix, not as an incompatible change,
      so the semantic version will not require a major version bump.

  * When testing, test with switch arguments, too.

* In ``env_utils``:

  * Add ``FF_ENV_PRODUCTION_BLUE_NEW`` (value ``'fourfront-production-blue'``)
    and ``FF_ENV_PRODUCTION_GREEN_NEW`` (value ``'fourfront-production-green'``)
    and teach ``is_stg_or_prd_env`` and ``get_standard_mirror_env`` about them
    as alternative stg/prd hosts.

  * Add ``is_beanstalk_env`` to detect traditional/legacy beanstalk names during transition.

* In ``qa_utils``:

  * Add ``MockedCommandArgs``.


3.12.0
======

* In ``diff_utils``:

  * Add support for ``.diffs(..., include_mappings=True)``
  * Add support for ``.diffs(..., normalizer=<fn>)`` where ``<fn>`` is a function of two keyword arguments,
    ``item`` and ``label`` that can rewrite a given expression to be compared into a canonical form (e.g.,
    reducing a dictionary with a ``uuid`` to just the ``uuid``, which is what we added the functionality for).


3.11.1
======

* In ``ff_utils``:

  * In ``get_metadata``, strip leading slashes on ids in API functions.


3.11.0
======

* Adds support for ``creds_utils``.

  * Class ``KeyManager``, with methods:

    * ``KeyManager.get_keydict_for_env(self, env)``

    * ``KeyManager.get_keydict_for_server(self, server)``

    * ``KeyManager.get_keydicts(self)``

    * ``KeyManager.get_keypair_for_env(self, env)``

    * ``KeyManager.get_keypair_for_server(self, server)``

    * ``KeyManager.keydict_to_keypair(auth_dict)``

    * ``KeyManager.keypair_to_keydict(auth_tuple, *, server)``

  * Class ``FourfrontKeyManager``

  * Class ``CGAPKeyManager``


3.10.0
======

* In ``docker_utils.py``:

  * Add ``docker_is_running`` predicate (used by the fix to ``test_ecr_utils_workflow`` to skip that test
    if docker is not running.

* In ``test_ecr_utils.py``:

  * Fix ``test_ecr_utils_workflow`` to skip if docker is not enabled.

* In ``test_s3_utils.py``:

  * Remove ``test_s3utils_creation_cgap_ordinary`` because there are no more CGAP beanstalks.
  * Revise ``test_regression_s3_utils_short_name_c4_706`` to use ``fourfront-mastertest``
    rather than a CGAP env, since the CGAP beanstalk envs have gone away.

* In ``qa_utils.py``:

  * ``MockBoto3Session``.
  * ``MockBoto3SecretsManager`` and support for ``MockBoto3`` to make it.

* In ``secrets_utils.py`` and ``test_secrets_utils.py``:

  * Add support for ``SecretsTable``.
  * Add unit tests for existing ``secrets_utils.assume_identity`` and for new ``SecretsTable`` functionality.

* Small cosmetic adjustments to ``Makefile`` to show a timestamp and info about current branch state
  when ``make test`` starts and again when it ends.

* A name containing an underscore will not be shortened by ``short_env_name`` nor lengthened by
  ``full_env_name`` (nor ``full_cgap_env_name`` nor ``full_fourfront_env_name``).


3.9.0
=====

* Allow dcicutils to work in Python 3.9.


3.8.0
=====

* Allow dcicutils to work in Python 3.8.


3.7.1
=====

* In ``ecs_utils``:

  * No longer throw exception when listing services if <4 are returned


3.7.0
=====

* In ``s3_utils``:

  * Add ``HealthPageKey.PYTHON_VERSION``


3.6.1
=====

* In ``ecs_utils``:

  * Add ``list_ecs_tasks``
  * Add ``run_ecs_task``


3.6.0
=====

* In ``string_utils``:

  * Add ``string_list``
  * Add ``string_md5``


3.5.0
=====

* In ``ff_utils``:

  * Add ``parse_s3_bucket_and_key_url``.


3.4.2
=====

* In ``qa_utils``:

  * In ``MockBotoS3Client``:

    * Fix ``head_object`` operation to return the ``StorageClass``
      (since the mock already allows you to declare it per-S3-client-class).

    * Add internal support to be expanded later for making individual S3 files
      have different storage classes from one another.


3.4.1
=====

* ``deployment_utils``:

  * Default the value of ``s3_encode_key_id`` to the empty string, not ``None``.


3.4.0
=====

* In ``deployment_utils``:

  * Add ``create_file_from_template``.

* In ``qa_utils``:

  * Fix an obscure bug in ``os.remove`` mocking by ``MockFileSystem``.

* In ``s3_utils``:

  * Add ``s3Utils.s3_encrypt_key_id``.
  * Add ``HealthPageKey.S3_ENCRYPT_KEY_ID``.

* In ``test/test_base.py``:

  * Disable unit tests that are believed broken by WAF changes.

    * ``test_magic_cnames_by_production_ip_address``
    * ``test_magic_cnames_by_cname_consistency``


3.3.0
=====

* Add support for environment variable ``ENCODED_S3_ENCRYPT_KEY_ID``, to allow ``S3_ENCRYPT_KEY_ID`` in ``.ini`` files.


3.2.1
=====

* Codebuild support


3.2.0
=====

* In ``command_utils``:

  * Allow a ``no_execute`` argument to ``ShellScript`` to suppress all evaluation.
    (This is subprimitive. Most users still want ``simulate=``)

  * New context manager method ``ShellScript.done_first`` usable in place of ``ShellScript.do_first`` when there are several things to go at the start, so that they can execute forward instead of backward.

  * New function ``setup_subrepo`` to download a repository and set up its virtual env.

    * New function ``script_assure_env`` to help with that.


3.1.0
=====

This PR is intended to phase out any importation of named constants from ``env_utils``.
Named functions are preferred.

* New module ``common`` for things that might otherwise go in ``base`` but are OK to import.
  (The ``base`` module is internal and not for use outside of ``dcicutils``.)

  * Moved ``REGION`` from ``base`` to ``common``, leaving behind an import/exported pair for compatibility,
    but please import ``REGION`` from ``dcicutils.common`` going forward.

  * ``OrchestratedApp`` and ``EnvName`` for type hinting.

  * ``APP_CGAP`` and ``APP_FOURFRONT`` as a more abstract way of referring to ``'cgap'`` and ``'fourfront'``,
    respectively, to talk about which orchestrated app is in play.

* In ``env_utils``:

  * New function ``default_workflow_env`` for use in CGAP and Fourfront functions ``run_workflow`` and ``pseudo_run``
    (in ``src/types/workflow.py``) so that ``CGAP_ENV_WEBDEV`` and ``FF_ENV_WEBDEV`` do not need to be imported.

  * New function ``infer_foursight_url_from_env``, similar to ``infer_foursight_from_env`` but returns a URL
    rather than an environment short name.

  * New function ``short_env_name`` that computes the short name of an environment.

  * New function ``test_permit_load_data`` to gate whether a ``load-data`` command should actually load any data.

  * New function ``prod_bucket_env_for_app`` to return the prod_bucket_env for an app.

  * New function ``public_url_for_app`` to return the public production URL for an app.


3.0.1
=====

* In ``env_utils``:

  * A small bit of error checking in ``blue_green_mirror_env``.

  * A bit of extra testing for ``infer_foursight_from_env``.


3.0.0
=====

The major version bump is to allow removal of some deprecated items
and to further constrain the Python version.

Strictly speaking, this is an **INCOMPATIBLE CHANGE**, though we expect little or no
impact.

In particular, searches of all ``4dn-dcic`` and ``dbmi-cgap`` repositories on GitHub show
that only the ``torb`` repository is impacted, and since that repo is not
in active use, we're not worried about that. Also, minor code adjustments would
fix the problem uses allowing uses of version 3.0 or higher.

Specifics:

* Supports versions of Python starting with 3.6.1 and below 3.8.

* Removes support for previously-deprecated function name ``whodaman``, which only ``torb`` was still using.
  ``compute_ff_prd_env`` can be used as a direct replacement.

* Removes support for previously-deprecated variable ``MAGIC_CNAME`` which no one was using any more.

* Removes support for previously-deprecated variable ``GOLDEN_DB`` which only ``torb`` was still using.
  ``_FF_GOLDEN_DB`` could be used as a direct replacement in an emergency,
  but only for legacy environments. This is not a good solution for orchestrated environments
  (`C4-689 <https://hms-dbmi.atlassian.net/browse/C4-689>`_).

* The variables ``FF_MAGIC_CNAME``, ``CGAP_MAGIC_CNAME``, ``FF_GOLDEN_DB``, and ``CGAP_GOLDEN_DB``,
  which had no uses outside of ``dcicutils`` itself,
  now have underscores ahead of their names to emphasize that they are internal to ``dcicutils`` only.
  ``_FF_MAGIC_CNAME``, ``_CGAP_MAGIC_CNAME``, ``_FF_GOLDEN_DB``, and ``_CGAP_GOLDEN_DB``, respectively,
  could be used as a direct replacement in an emergency,
  but only for legacy environments. This is not a good solution for orchestrated environments
  (`C4-689 <https://hms-dbmi.atlassian.net/browse/C4-689>`_).

* The function name ``use_input`` has been renamed ``prompt_for_input`` and the preferred place to
  import it from is now ``misc_utils``, not ``beanstalk_utils``. (This is just a synonym for the
  poorly named Python function ``input``.)

* The previously-deprecated class name ``deployment_utils.Deployer`` has been removed.
  ``IniFileManager`` can be used as a direct replacement.

* The previously-deprecated function name ``guess_mirror_env`` has been removed.
  ``get_standard_mirror_env`` can be used as a direct replacement.

* The deprecated function name ``hms_now`` and the deprecated variable name ``HMS_TZ`` have been removed.
  ``ref_now`` and ``REF_TZ``, respectively, can be used as direct replacements.

* These previously-deprecated ``s3_utils.s3Utils`` class variables have been removed:

  * ``s3Utils.SYS_BUCKET_HEALTH_PAGE_KEY`` replaced by ``HealthPageKey.SYSTEM_BUCKET``
  * ``s3Utils.OUTFILE_BUCKET_HEALTH_PAGE_KEY`` replaced by ``HealthPageKey.PROCESSED_FILE_BUCKET``
  * ``s3Utils.RAW_BUCKET_HEALTH_PAGE_KEY`` replaced by ``HealthPageKey.FILE_UPLOAD_BUCKET``
  * ``s3Utils.BLOB_BUCKET_HEALTH_PAGE_KEY`` replaced by ``HealthPageKey.BLOB_BUCKET``
  * ``s3Utils.METADATA_BUCKET_HEALTH_PAGE_KEY`` replaced by ``HealthPageKey.METADATA_BUNDLES_BUCKET``
  * ``s3Utils.TIBANNA_OUTPUT_BUCKET_HEALTH_PAGE_KEY`` replaced by ``HealthPageKey.TIBANNA_OUTPUT_BUCKET``

  Among ``4dn-dcic`` repos, there was only one active use of any of these, ``TIBANNA_OUTPUT_BUCKET_HEALTH_PAGE_KEY``,
  in ``src/commands/setup_tibanna.py`` in ``4dn-cloud-infra``. It will need to be rewritten.

  Among ``dbmi-bgm`` repos, all are mentioned only in ``src/encoded/root.py`` and ``src/encoded/tests/test_root.py``,
  but rewrites to use ``HealthPageKey`` attributes will be needed there as well.


2.4.1
=====

* No functional change. Cosmetic edits to various files in order to
  make certain file comparisons tidier.


2.4.0
=====

* This change rearranges files to remove some bootstrapping issues caused by circular dependencies.
  This change is not supposed to affect the visible behavior, but the nature of the change creates
  a risk of change because things moved from file to file.
  An attempt was made to retain support for importable functions and variables in a way that would be non-disruptive.

* New module ``ff_mocks`` containing some test facilities that can be used by other repos to test FF and CGAP stuff.

  * Class ``MockBoto4DNLegacyElasticBeanstalkClient``.

  * Context manager ``mocked_s3utils`` for mocking many typical situations.

2.3.2
=====

* Support Central European Time for testing.


2.3.1
=====

* In ``s3_utils``, fix `C4-706 <https://hms-dbmi.atlassian.net/browse/C4-706>`_,
  where short names of environments were not accepted as env arguments to s3Utils in legacy CGAP.


2.3.0
=====

* In ``qa_utils`` add some support for testing new functionality:

  * In ``MockBoto3``, create a different way to register client classes.

  * In ``MockBotoS3Client``:

    * Add minimal support for ``head_bucket``.
    * Add minimal support for ``list_objects_v2``.
    * Make ``list_objects`` and ``list_objects_v2``, return a ``KeyCount`` in the result.

  * New class ``MockBotoElasticBeanstalkClient`` for mocking beanstalk behavior.

    * New subclasses ``MockBoto4DNLegacyElasticBeanstalkClient`` and ``MockBotoFooBarElasticBeanstalkClient``
      that mock behavior of our standard legacy setup and a setup with just a ``fourfront-foo`` and ``fourfront-bar``,
      respectively.

* In ``s3_utils``:

  * Add a class ``HealthPageKey`` that holds names of keys expected in health page json.
    This was ported from ``cgap-portal``, which can now start importing from here.
    Also:

    * Add ``HealthPageKey.TIBANNA_CWLS_BUCKET``.

  * In ``s3Utils``:

    * Add ``TIBANNA_CWLS_BUCKET_SUFFIX``.

  * Add an ``EnvManager`` object to manage obtaining and parsing contents of the data in global env bucket.
    Specific capabilities include:

    * Static methods ``.verify_and_get_env_config()`` and ``.fetch_health_page_json()`` moved from ``s3Utils``.
      (Trampoline functions have been left behind on that class for compatibility.)

    * Static method ``.global_env_bucket_name()`` to get the current global env bucket environment variable.

    * Static method (and context manager) ``.global_env_bucket_named(name=...)`` to bind the name of the current
      global env bucket using Python's ``with``.

    * Virtual attributes ``.portal_url``, ``.es_url``, and ``env_name`` for accessing the contents of the dictionary
      obtained from the global env bucket.

    * This class also creates suitable abstraction to allow for a future in which the contents of this dictionary
      might include keys ``portal_url``, ``es_url``, and ``env_name`` in lieu of what are now
      ``fourfront``, ``es``, and ``ff_env``, respectively.

    * When an ``env`` argument is given in creation of ``s3Utils``, an ``EnvManager`` object will be placed in
      the ``.env_manager`` property of the resulting ``s3Utils`` instance. (If no ``env`` argument is given, no
      such object can usefully be created since there is insufficient information.)

* In ``deployment_utils``:

  * Support ``ENCODED_TIBANNA_CWLS_BUCKET`` and a ``--tibanna-cwls-bucket`` command line argument that get merged
    into ``TIBANNA_CWLS_BUCKET`` for use in ``.ini`` templates.  These default similarly to how the
    Tibanna output bucket does.


2.2.1
=====

* In ``env_utils``:

  * Add ``fourfront-cgap`` to the table of ``CGAP_PUBLIC_URLS``.


2.2.0
=====

* In ``cloudformation_utils``:

  * Add ``hyphenify`` to change underscores to hyphens.

* In ``command_utils``:

  * Add ``shell_script`` context manager and its implementation class ``ShellScript``.

  * Add ``module_warnings_as_ordinary_output`` to help work around the problem that S3Utils outputs
    text we'd sometimes rather see as ordinary output, not log output.

* In ``lang_utils``:

  * Add support for ``string_pluralize`` to pluralize 'nouns' that have attached prepositional phrases, as in::

       string_pluralize('file to load')
       'files to load`

       string_pluralize('brother-in-law of a proband')
       'brothers-in-law of probands'

       string_pluralize('brother-in-law of the proband')
       'brothers-in-law of the proband'

    But, importantly, this also means one can give have arguments to functions that use these do something
    sophisticated in terms of wording with almost no effort at the point of need, such as::

       [there_are(['foo.json', 'bar.json'][:n], kind='file to load') for n in range(3)]
       [
         'There are no files to load.',
         'There is 1 file to load: foo.json',
         'There are 2 files to load: foo.json, bar.json'
       ]

       [n_of(n, 'bucket to delete') for n in range(3)]
       [
         '0 buckets to delete',
         '1 bucket to delete',
         '2 buckets to delete'
       ]

* Miscellaneous other changes:

  * In ``docs/source/dcicutils.rst``, add autodoc for various modules that are not getting documented.

  * In ``test/test_misc.py``, add unit test to make sure things don't get omitted from autodoc.

    Specifically, a test will now fail if you make a new file in ``dcicutils`` and do not add a
    corresponding autodoc entry in ``docs/source/dcicutils.rst``.


2.1.0
=====

* In ``s3_utils``, add various variables that can be used to assure values are synchronized across 4DN/CGAP products:

  * Add new slots on ``s3Utils`` to hold the token at the end of each kind of bucket:

    * ``s3Utils.SYS_BUCKET_SUFFIX == "system"``
    * ``s3Utils.OUTFILE_BUCKET_SUFFIX == "wfoutput"``
    * ``s3Utils.RAW_BUCKET_SUFFIX == "files"``
    * ``s3Utils.BLOB_BUCKET_SUFFIX == "blobs"``
    * ``s3Utils.METADATA_BUCKET_SUFFIX == "metadata-bundles"``
    * ``s3Utils.TIBANNA_OUTPUT_BUCKET_SUFFIX == 'tibanna-output'``

  * Add new slots on ``s3Utils`` for various bits of connective glue in setting up the template slots:

    * ``s3Utils.EB_PREFIX == "elasticbeanstalk"``
    * ``s3Utils.EB_AND_ENV_PREFIX == "elasticbeanstalk-%s-"``

  * Add new slots on ``s3Utils`` for expected keys on a health page corresponding to each kind of bucket:

    * ``s3Utils.SYS_BUCKET_HEALTH_PAGE_KEY == 'system_bucket'``
    * ``s3Utils.OUTFILE_BUCKET_HEALTH_PAGE_KEY == 'processed_file_bucket'``
    * ``s3Utils.RAW_BUCKET_HEALTH_PAGE_KEY == 'file_upload_bucket'``
    * ``s3Utils.BLOB_BUCKET_HEALTH_PAGE_KEY == 'blob_bucket'``
    * ``s3Utils.METADATA_BUCKET_HEALTH_PAGE_KEY == 'metadata_bundles_bucket'``
    * ``s3Utils.TIBANNA_OUTPUT_BUCKET_HEALTH_PAGE_KEY == 'tibanna_output_bucket'``

* In ``deployment_utils``, use new variables from ``s3_utils``.


2.0.0
=====

`PR 150: Add json_leaf_subst, conjoined_list and disjoined_list <https://github.com/4dn-dcic/utils/pull/150>`_

We do not believe this is an incompatible major version, but there is a lot here, an hence some opportunity for
difference in behavior to have crept in. As such, we opted to call this a new major version to highlight where
that big change happened.

* In ``beanstalk_utils``:

  * Add ``'elasticbeanstalk-%s-metadata-bundles'`` to the list of buckets that ``beanstalk_utils.delete_s3_buckets``
    is willing to delete.

* In ``cloudformation_utils``:

  * New functions ``camelize`` and ``dehyphenate`` because they're needed a lot in our ``4dn-cloud-infra`` repo.

  * New implementation of functions ``get_ecs_real_url`` and ``get_ecr_repo_url`` that are not Alpha-specific.

  * New classes ``AbstractOrchestrationManager``, ``C4OrchestrationManager``, and ``AwsemOrchestrationManager``
    with various utilities ported from ``4dn-cloud-infra`` (so they could be used to re-implement
    ``get_ecs_real_url``and ``get_ecr_repo_url``).

  * New ``test_cloudformation_utils.py`` testing each of the bits of functionality in ``cloudformation_utils``
    along normal paths, including sometimes mocking both the Alpha and KMP environments, hoping transitions
    will be smooth.

* In ``deployment_utils``:

  * Support environment variable ``ENCODED_IDENTITY`` and ``--identity`` to control
    environment variable ``$IDENTITY`` in construction of ``production.ini``.

  * Support environment variable ``ENCODED_TIBANNA_OUTPUT_BUCKET`` and ``--tibanna_output_bucket`` to control
    environment variable ``$TIBANNA_OUTPUT_BUCKET`` in construction of ``production.ini``.

  * Support environment variable ``ENCODED_APPLICATION_BUCKET_PREFIX`` and ``--application_bucket_prefix`` to control
    environment variable ``$APPLICATION_BUCKET_PREFIX`` in construction of ``production.ini``.

  * Support environment variable ``ENCODED_FOURSIGHT_BUCKET_PREFIX`` and ``--foursight_bucket_prefix`` to control
    environment variable ``$FOURSIGHT_BUCKET_PREFIX`` in construction of ``production.ini``.

  * New class variable ``APP_KIND`` in ``IniFileManager``.
    Default is ``None``, but new subclasses adjust the default to ``cgap`` or ``fourfront``.

  * New class variable ``APP_ORCHESTRATED`` in ``IniFileManager``.
    Default is ``None``, but new subclasses adjust the default to ``True`` or ``False``.

  * New classes

    * ``BasicCGAPIniFileManager``
    * ``BasicLegacyCGAPIniFileManager``
    * ``BasicOrchestratedCGAPIniFileManager``
    * ``BasicFourfrontIniFileManager``
    * ``BasicLegacyFourfrontIniFileManager``
    * ``BasicOrchestratedFourfrontIniFileManager``

    In principle, this should allow some better defaulting.

* In ``exceptions``:

  * Add ``InvalidParameterError``.

* In ``lang_utils``:

  * Add ``conjoined_list`` and ``disjoined_list`` to get a comma-separated
    list in ordinary English form with an "and" or an "or" before the
    last element. (Note that these also support new functions
    ``there_are`` and ``must_be_one_of``).

  * Add ``there are`` and ``must_be_one_of`` to handle construction of
    messages that are commonly needed but require nuanced adjustment of
    wording to sound right in English. (Note that ``must_be_one_of`` also
    supports ``InvalidParameterError``.)

* In ``misc_utils``:

  * Add ``json_leaf_subst`` to do substitutions at the leaves
    (atomic parts) of a JSON object.

  * Add ``NamedObject`` for creating named tokens.

  * Add a ``separator=`` argument to ``camel_case_to_snake_case`` and ``snake_case_to_camel_case``.

* In ``qa_utils``, support for mocking enough of ``boto3.client('cloudformation')`` that we can test
  ``cloudformation_utils``. The ``MockBoto3Client`` was extended, and several mock classes were added,
  but most importantly:

  * ``MockBotoCloudFormationClient``
  * ``MockBotoCloudFormationStack``
  * ``MockBotoCloudFormationResourceSummary``

* In ``s3_utils``:

  * Make initialize attribute ``.metadata_bucket`` better.

  * Add an attribute ``.tibanna_output_bucket``


1.20.0
======

`PR 148: Support auth0 client and secret in deployment_utils <https://github.com/4dn-dcic/utils/pull/148>`_

* In ``deployment_utils``, add support for managing auth0 client and secret:

  * To pass client and secret into the ini file generator:

    * ``--auth0_client`` and ``--auth0_secret`` command line arguments.
    * ``$ENCODED_AUTH0_CLIENT`` and ``ENCODED_AUTH0_SECRET`` as environment variables.

  * Ini file templates can just use ``AUTH0_CLIENT`` and ``AUTH0_SECRET`` to obtain a properly defaulted value.
    It is recommended to put something like this in the ini file template::

      auth0.client = ${AUTH0_CLIENT}
      auth0.secret = ${AUTH0_SECRET}


1.19.0
======

`PR 147: Init s3Utils via GLOBAL_ENV_BUCKET and misc S3_BUCKET_ORG support (C4-554) <https://github.com/4dn-dcic/utils/pull/147>`_
`PR 146: Better S3 bucket management in deployment_utils <https://github.com/4dn-dcic/utils/pull/146>`_

* In ``cloudformation_utils``:

  * Small bug fix to ``get_ecs_real_url``.

  * Add ``get_ecr_repo_url``.

* In ``deployment_utils``:

  * Add environment variables that can be set per stack/instance:

    * ``ENCODED_S3_BUCKET_ORG`` - a unique token for your organization to be used in auto-generating S3 bucket orgs.
      The defaulted value (which includes possible override by a ``--s3_bucket_org`` argument in the generator command)
      will be usable as ``${S3_BUCKET_ORG}`` in ``.ini`` file templates.

    * ``ENCODED_S3_BUCKET_ENV`` - a unique token for your organization to be used in auto-generating S3 bucket names.
      The defaulted value (which includes possible override by a ``--s3_bucket_env`` argument in the generator command)
      will be usable as ``${S3_BUCKET_ENV}`` in ``.ini`` file templates.

    * ``ENCODED_FILE_UPLOAD_BUCKET`` - the name of the file upload bucket to use if a ``--file_upload_bucket`` argument
      is not given in the generator command, and the default of ``${S3_BUCKET_ORG}-${S3_BUCKET_ENV}-files``
      is not desired. This fully defaulted value will be available as ``${FILE_UPLOAD_BUCKET}`` in ``.ini`` file
      templates, and is the recommended way to compute the proper value for the ``file_upload_bucket`` configuration
      parameter.

    * ``ENCODED_FILE_WFOUT_BUCKET`` - the name of the file wfout bucket to use if a ``--file_wfout_bucket`` argument
      is not given in the generator command, and the default of ``${S3_BUCKET_ORG}-${S3_BUCKET_ENV}-wfoutput``
      is not desired. This fully defaulted value will be available as ``${FILE_WFOUT_BUCKET}`` in ``.ini`` file
      templates, and is the recommended way to compute the proper value for the ``file_wfout_bucket`` configuration
      parameter.

    * ``ENCODED_BLOB_BUCKET`` - the name of the blob bucket to use if a ``--blob_bucket`` argument
      is not given in the generator command, and the default of ``${S3_BUCKET_ORG}-${S3_BUCKET_ENV}-blobs``
      is not desired. This fully defaulted value will be available as ``${BLOB_BUCKET}`` in ``.ini`` file
      templates, and is the recommended way to compute the proper value for the ``blob_bucket`` configuration
      parameter.

    * ``ENCODED_SYSTEM_BUCKET`` - the name of the system bucket to use if a ``--system_bucket`` argument
      is not given in the generator command, and the default of ``${S3_BUCKET_ORG}-${S3_BUCKET_ENV}-system``
      is not desired. This fully defaulted value will be available as ``${SYSTEM_BUCKET}`` in ``.ini`` file
      templates, and is the recommended way to compute the proper value for the ``system_bucket`` configuration
      parameter.

    * ``ENCODED_METADATA_BUNDLES_BUCKET`` - the name of the metadata bundles bucket to use if a
      ``--metadata_bundles_bucket`` argument is not given in the generator command, and the default of
      ``${S3_BUCKET_ORG}-${S3_BUCKET_ENV}-metadata-bundles`` is not desired. This fully defaulted value will be
      available as ``${METADATA_BUNDLES_BUCKET}`` in ``.ini`` file
      templates, and is the recommended way to compute the proper value for the ``metadata_bundles_bucket`` configuration
      parameter.

    * Fixed a bug that the index_server argument was not being correctly passed into lower level functions when
      ``--index_server`` was specified on the command line.

    * Fixed a bug where passing no ``--encoded_data_set`` but an explicit null-string value of the environment variable
      ``ENCODED_DATA_SET`` did not lead to further defaulting in some circumstances.

  * In ``ff_utils``:

    * Add ``fetch_network_ids``.

  * In ``misc_utils``:

    * Add ``dict_zip``.

  * In ``s3_utils``:

    * Add new methods ``fetch_health_page_json`` and ``verify_and_Get_env_config`` in support of new initialization
      protocol for ``s3Utils``.

    * Extend ``s3Utils`` initialization protocol so that under certain conditions,
      environment variable if ``GLOBAL_ENV_BUCKET`` is set,
      the init protocol will be discovered from that bucket.

      NOTE WELL: The name ``GLOBAL_BUCKET_ENV`` is also supported as a synonm for ``GLOBAL_ENV_BUCKET``
      because it was used in testing before we settled on a final name, and we're allowing a
      grace period. But this name should not be considered properly supported. That it works now
      is a courtesy and anyone concerned about incompatible changes should use the newer name,
      ``GLOBAL_ENV_BUCEKT``.


1.18.1
======

`PR 145: Fix internal import problems <https://github.com/4dn-dcic/utils/pull/145>`_

* Make ``lang_utils`` import ``ignored`` from ``misc_utils``, not ``qa_utils``.
* Make ``deployment_utils`` import ``override_environ`` from ``misc_utils``, not ``qa_utils``.
* Move ``local_attrs`` from ``qa_utils`` to ``misc_utils``
  so that similar errors can be avoided in other libraries that import it.


1.18.0
======

`PR 141: Port Application Dockerization utils <https://github.com/4dn-dcic/utils/pull/141>`_

* Add additional ECS related APIs needed for orchestration/deployment.


1.17.0
======

`PR 144: Add known_bug_expected and related support <https://github.com/4dn-dcic/utils/pull/144>`_

* In ``misc_utils``:

  * Add ``capitalize1`` to uppercase the first letter of something,
    leaving other case alone (rather than forcing it lower).

* In ``qa_utils``:

  * Add ``known_bug_expected`` to mark situations in testing where
    a named bug is expected (one for which there is a JIRA ticket),
    allowing managing of the error handling by setting the bug's status
    as ``fixed=False`` (the default) or ``fixed=True``.

* In (new module) ``exceptions``:

  * ``KnownBugError``
  * ``UnfixedBugError``
  * ``WrongErrorSeen``
  * ``ExpectedErrorNotSeen``
  * ``FixedBugError``
  * ``WrongErrorSeenAfterFix``
  * ``UnexpectedErrorAfterFix``


1.16.0
======

`PR 142: Move override_environ and override_dict to misc_utils <https://github.com/4dn-dcic/utils/pull/142>`_

* In ``misc_utils``:

  * Adds ``override_environ`` and ``override_dict``
    which were previously defined in ``qa_utils``.

  * Adds new function ``exported`` which is really a synonym
    for ``ignored`` but highlights the reason for the presence
    of the named variable is so that other files can still
    import it.

* In ``qa_utils``:

  * Leaves legacy support for ``override_environ``
    and ``override_dict``, which are now defined in ``misc_utils``.


1.15.1
======

`PR 138: JH Docker Mount Update <https://github.com/4dn-dcic/utils/pull/138>`_

* In ``jh_utils.find_valid_file_or_extra_file``,
  account for file metadata containing an
  ``"open_data_url"``.


1.15.0
======

`PR 140: Add misc_utils.is_valid_absolute_uri (C4-651) <https://github.com/4dn-dcic/utils/pull/140>`_

* Adds ``misc_utils.is_valid_absolute_uri``
  for RFC 3986 compliance.


1.14.1
======

`PR 139: Add ES cluster resize capability <https://github.com/4dn-dcic/utils/pull/139>`_

* Adds ElasticSearchServiceClient, a wrapper for boto3.client('es')
* Implements resize_elasticsearch_cluster, issuing an update to the relevant settings
* Integrated test was performed on staging
* Unit tests mock the boto3 API


1.14.0
======

`PR 137: Docker, ECR, ECS Utils <https://github.com/4dn-dcic/utils/pull/137>`_

* Adds 3 new modules with basic functionality needed for further development on the alpha stack
* Deprecates Python 3.4


1.13.0
======

`PR 136: Support for VirtualApp.post <https://github.com/4dn-dcic/utils/pull/136>`_

* Add a ``post`` method to ``VirtualApp`` for situations where ``post_json``
  is not appropriate.



1.12.0
======

`PR 135: Support for ElasticSearchDataCache <https://github.com/4dn-dcic/utils/pull/135>`_

* Support for ``ElasticSearchDataCache`` and the ``es_data_cache`` decorator
  in the new ``snapshot_utils`` module to allow local snapshot isolation on
  tests. For now this feature is entirely OFF unless one uses environment
  variable ENABLE_SNAPSHOTS=TRUE in the command invocation.

* Extend the mock for ``open`` in ``qa_utils.MockFileSystem`` to handle
  file open modes involving "t" and "+".

* Support for ``qa_utils.MockFileSystem``:

  * New keyword arguments
    ``auto_mirror_files_for_read`` and ``do_not_auto_mirror``.

  * New context manager method ``mock_exists_open_remove`` that mocks these
    common methods for the mock file system that is its ``self``.

* In ``misc_utils``:

  * Extend ``find_association`` to allow a predicate as a search value.

  * New function ``find_associations`` which is like ``find_association``
    but returns a list of results, so doesn't err if more than one found.


1.11.2
======

`PR 134: Fixes to env_utils.data_set_for_env for CGAP (C4-634) <https://github.com/4dn-dcic/utils/pull/134>`_

* Fix ``env_utils.data_set_for_env`` which were returning ``'test'``
  for ``fourfront-cgapwolf`` and ``fourfront-cgaptest``.
  Oddly, the proper value is ``'prod'``.


1.11.1
======

`PR 133: Fix ControlledTime.utcnow on AWS (C4-623) <https://github.com/4dn-dcic/utils/pull/133>`_

* Fix ``qa_utils.ControlledTime.utcnow`` on AWS (C4-623).


1.11.0
======

`PR 132: Miscellaneous support for cgap-portal, and some unit testing (part of C4-601) <https://github.com/4dn-dcic/utils/pull/132>`_

* For ``jh_utils``:

  * Better unit test for ``find_valid_file_or_extra_file`` (part of fixing C4-601).

* For ``misc_utils``:

  * New function ``ignorable`` which is basically a synonym for ``ignore``, but with the sense that it's OK for the variables given as its arguments to be used elsewhere or not.
  * New function ``ancestor_classes`` that returns a list of the classes from which a given class inherits.
  * New function ``is_proper_subclass`` that is like ``issubclass`` but returns ``True`` only if its two arguments _are_ not the same class.
  * New function ``identity`` that returns its argument.
  * New functions ``count`` and ``count_if`` for counting things in a sequence.
  * New function ``find_association`` for finding dictionaries in a list based on specified field criteria.
  * New ``@decorator`` decorator for defining (what else?) decorators. Specifically, this addresses the ``@foo`` vs ``@foo()`` issue, allowing both syntaxes.


1.10.0
======

`PR 131: Misc functionality in service of C4-183 <https://github.com/4dn-dcic/utils/pull/131>`_

* In ``dcicutils.misc_utils``:

  * New function ``remove_element`` to remove an element from a list.
  * New class ``TestApp`` which is a synonym for ``webtest.TestApp``
    but declared not to be a test case.
  * Make ``_VirtualAppHelper`` use new ``TestApp``.


1.9.2
=====
`PR 130: Fix bug that sometimes results in duplicated search results (C4-336) <https://github.com/4dn-dcic/utils/pull/130>`_

* Fixes bug C4-336, in which sometimes ``ff_utils.search_metadata``, by doing a series of
  Elastic Search calls that it pastes together into a single result,
  can return a list containing duplicated items.


1.9.1
=====

`PR 129: Fix problematic pytest dependency (C4-521) <https://github.com/4dn-dcic/utils/pull/129>`_

* Fix problem in 1.9.0 with unwanted dependency on
  ``pytest.PytestConfigWarning`` (C4-521).
* Added some unit tests to run instead of integration tests for
  ``s3_utils`` in a number of cases.


1.9.0
=====

`PR 128: Changelog Warnings (C4-511) and Publish Fixes (C4-512) <https://github.com/4dn-dcic/utils/pull/128>`_

* Make changelog problems issue a warning rather than fail testing.
* Make publication for GitHub Actions (GA) not query interactively for confirmation.

Some other fixes are included because the ``test_unzip_s3_to_s3``
and ``test_unzip_s3_to_s3_2`` tests were intermittently failing.
Those tests were refactored, and the following additional support was added:

* In ``MockBotoS3Client``, added support for some cases of:
  * ``.put_object()``
  * ``.list_objects()``


1.8.4
=====

`PR 127: Beanstalk Bugfix <https://github.com/4dn-dcic/utils/pull/127>`_

* Parses Beanstalk API correctly and passes region.


1.8.3
=====

**No PR: Just fixes to GA PyPi deploy**


1.8.2
=====

`PR 126: C4-503 Grab Environment API <https://github.com/4dn-dcic/utils/pull/126>`_

* Adds get_beanstalk_environment_variables, which will return information
  necessary to simulate any application given the caller has the appropriate
  access keys.
* Removes an obsolete tag from create_db_snapshot, which was set erroneously.


1.8.1
=====

`PR 125: Edits to getting_started doc <https://github.com/4dn-dcic/utils/pull/125>`_

* Edited getting_started.rst doc to reflect updated account creation protocol.


1.8.0
=====

`PR 124: Add url_path_join <https://github.com/4dn-dcic/utils/pull/124>`_

* Add ``misc_utils.url_path_join`` for merging parts of URLs.
* Add ``make retest`` to rerun failed tests from previous test run.


1.7.1
=====

`PR 123: Add GA for build <https://github.com/4dn-dcic/utils/pull/123>`_

* Adds 3 Github Actions for building the library, building docs
  and deploying to PyPi


1.7.0
=====

`PR 122: Speed up ff_utils unit tests, and misc small bits of functionality <https://github.com/4dn-dcic/utils/pull/122>`_

* Added an ``integratedx`` mark to possible marks in ``pytest.ini``. These
  are the same as ``integrated`` but they represent test cases that have
  an associated unit test that is redundant, so that the ``integratedx``
  test doesn't have to be run to get full coverage.

* For ``ff_utils``:

  * Split tests into a ``xxx_unit`` and
    ``xxx_integrated`` version.  The latter is marked with new
    ``integratedx`` mark.

* For ``env_utils``:

  * Added some test cases.

* For ``s3_utils``:

  * Small remodularization of ``s3Utils`` for easier access to
    some constants in testing.
  * Improvements to error reporting in ``s3Utils.get_access_keys()``.

* For ``qa_utils``:

  * In ``MockFileSystem``, fixed a typo in debugging typeout.
  * In ``MockResponse``:

    * Added a ``url=`` init arg and ``.url`` property.
    * Added a .text as synonym for ``.content``.

  * In ``MockBotoS3Client``:

    * Extended to handle ``region_name=``.
    * Added ``mock_other_required_arguments=`` and ``mock_s3_files=``
      init args for use in testing.
    * Added ``MockBotoS3Client``, add ``.get_object(Bucket, Key)``.

* For ``ff_utils``:

  * Used ``ValueError`` rather than ``Exception`` in several
    places errors are raised.
  * Some very small other refactoring was also done
    for modularity that should not affect behavior.


1.6.0
=====

`PR 121: More time functions <https://github.com/4dn-dcic/utils/pull/121>`_

In ``misc_utils``:

* Fix ``as_datetime`` to raise an error on bad input, allowing ``raise_error=False``
  to suppress that if needed.
* Add ``as_ref_datetime`` to convert times to the reference timezone (US/Eastern by default).
* Add ``as_utc_datetime`` to convert times to UTC.
* Extend ``in_datetime_interval`` to parse all string arguments using
  ``as_ref_datetime``.
* Rename ``HMS_TZ`` to ``REF_TZ``, but keep ``HMS_TZ`` as a synonym for compatibility for now.
* Rename ``hms_now`` to ``ref_now``, but again keep ``hms_now`` as a synonym for compatibility for now.

The rationale for these changes is that if we deploy at other locations, it may not be HMS that is relevant, so we could be at some place with another timezone.


1.5.1
=====

`PR 120: Update ES-py Version <https://github.com/4dn-dcic/utils/pull/120>`_

* Updates elasticsearch library to 6.8.1 to take a bug fix.


1.5.0
=====

`PR 119: More env_utils support** <https://github.com/4dn-dcic/utils/pull/119>`_

* Add ``env_utils.classify_server_url``.


1.4.0
=====

`PR 118: Various bits of functionality in support of 4dn-status (C4-363) <https://github.com/4dn-dcic/utils/pull/118>`_

* New feature in ``qa_utils``:

  * ControlledTime can now be used as a mock for the datetime module itself
    in some situations, though some care is required.

* New features in ``misc_utils``:

  * ``as_seconds`` so that, for example ``as_seconds(minutes=3)``
    can be used to get 180.
  * ``hms_now`` to get the value of ``datetime.datetime.now()``
    in HMS local time (EST or EDT as appropriate).
  * ``in_datetime_interval`` to test that a given time is within
    a given time interval.
  * ``as_datetime`` to coerce a properly formatted ``str`` to
    a ``datetime.datetime``.


1.3.1
=====

`PR 117: Repair handling of sentry_dsn in deployment_utils (C4-361) <https://github.com/4dn-dcic/utils/pull/117>`_

* Fixes to ``deployment_utils``:

  * Changes the handling of sentry DSN as an argument (``--sentry_dsn``)
    to the deployer.
  * Doesn't raise an error if environment variables collide but with the same value.
  * Uses better binding technology for binding environment variables.
  * Factors in a change to the tests to not use a deprecated
    name (Deployer changed to IniFileMaker) for one of the classes.
  * PEP8 adjustments.

* Fixes to ``qa_utils``:

  * Don't do changelog cross-check for beta versions.

* PEP8 adjustments to ``test_env_utils`` and ``test_s3_utils``.


1.3.0
=====

`PR 115: Miscellaneous fixes 2020-10-06 <https://github.com/4dn-dcic/utils/pull/115>`_

* Fix a lurking bug in ``beanstalk_utils`` where ``delete_db`` had the wrong scope.
* Add ``qa_utils.raises_regexp`` for conceptual compatibility with ``AssertRaises`` in ``unittest``.
* Add ``misc_utils.CustomizableProperty`` and companion ``misc_utils.getattr_customized``.
* Add ``qa_utils.override_dict``, factored out of ``qa_utils.override_environ``.
* Add ``qa_utils.check_duplicated_items_by_key`` to aid in error reporting for search results.
* Add ``qa_utils.MockUUIDModule`` for being able to mock ``uuid.uuid4()``.
* Add ``qa_utils.MockBoto3``.
* Add ``qa_utils.MockBotoSQSClient`` so that ``get_queue_url`` and ``get_queue_attributes`` can be used
  in testing of ``ff_utils.stuff_in_queue``.
* Add support for ``sentry_dsn`` and a ``ENCODED_SENTRY_DSN``
  beanstalk environment variable in ``deployment_utils``.
* In tests for ``ff_utils``, convert tests for ``search_metadata`` and ``stuff_in_queue``
  to be proper unit tests, to avoid some timing errors that occur during integration testing.


1.2.1
=====

`PR 114: Port some utility <https://github.com/4dn-dcic/utils/pull/114>`_

* New ``ff_utils`` functions
  for common pages/info we'd like to obtain:
  ``get_health_page``, ``get_counts_page``,
  ``get_indexing_status``, and ``get_counts_summary``.
* New ``CachedField`` facility.
* New ``misc_utils`` functions ``camel_case_to_snake_case``,
  ``snake_case_to_camel_case``, and ``make_counter``.


1.2.0
=====

`PR 113: Deprecations, updates + CNAME swap <https://github.com/4dn-dcic/utils/pull/113>`_

* Implements an ``obsolete`` decorator,
  applied to many functions in ``beanstalk_utils``.
* Fixes some functions in ``beanstalk_utils``
  that do not work with ES6
* Pull full ``CNAME`` swap code from ``Torb`` into ``dcicutils``.


`PR 112: Miscellaneous utilities ported from cgap-portal and SubmitCGAP repos <https://github.com/4dn-dcic/utils/pull/112>`_

This still has a beta version number 1.1.0b1.

Ported functionality from ``cgap-portal`` and ``SubmitCGAP`` repos:

* New functions in ``env_utils``: ``is_cgap_server`` and ``is_fourfront_server``.
* New functions ``misc_utils``: ``full_object_name``, ``full_class_name``, ``constantly``,
  ``keyword_as_title``, ``file_contents``.
* New classes in ``qa_utils``: ``MockResponse`` and ``MockBotoS3Client``.
* New functions in ``qa_utils``: ``printed_output`` (context manager),
* Extend ``lang_utils.n_of`` to take a list as its first
  argument without calling ``len``.
* Tests for ``misc_utils.VirtualApp.put_json``.


`PR 111: ES6 - Fix create_es_client <https://github.com/4dn-dcic/utils/pull/111>`_

This is a major change, with beta version number 1.0.0.b1:

* Fixes to ``es_utils.create_es_client``.


0.41.0
======

`PR 110: Add VirtualApp.put_json (C4-272) <https://github.com/4dn-dcic/utils/pull/110>`_

* Add ``misc_utils.VirtualApp.put_json``.


Older Versions
==============

A record of older changes can be found
`in GitHub <https://github.com/4dn-dcic/utils/pulls?q=is%3Apr+is%3Aclosed>`_.
To find the specific version numbers, see the ``version`` value in
the ``poetry.app`` section of ``pyproject.toml`` for the corresponding change, as in::

   [poetry.app]
   name = "dcicutils"
   version = "100.200.300"
   ...etc.


This would correspond with ``dcicutils 100.200.300``.
