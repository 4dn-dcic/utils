=========
dcicutils
=========

----------
Change Log
----------


1.12.0
======

**PR 135: Support for ElasticSearchDataCache**

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

**PR 134: Fixes to env_utils.data_set_for_env for CGAP (C4-634)**

* Fix ``env_utils.data_set_for_env`` which were returning ``'test'``
  for ``fourfront-cgapwolf`` and ``fourfront-cgaptest``.
  Oddly, the proper value is ``'prod'``.


1.11.1
======

**PR 133: Fix ControlledTime.utcnow on AWS (C4-623)**

* Fix ``qa_utils.ControlledTime.utcnow`` on AWS (C4-623).


1.11.0
======

**PR 132: Miscellaneous support for cgap-portal, and some unit testing (part of C4-601)**

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

**PR 131: Misc functionality in service of C4-183** 

* In ``dcicutils.misc_utils``:

  * New function ``remove_element`` to remove an element from a list.
  * New class ``TestApp`` which is a synonym for ``webtest.TestApp``
    but declared not to be a test case.
  * Make ``_VirtualAppHelper`` use new ``TestApp``.


1.9.2
=====
**PR 130: Fix bug that sometimes results in duplicated search results (C4-336)**

* Fixes bug C4-336, in which sometimes ``ff_utils.search_metadata``, by doing a series of
  Elastic Search calls that it pastes together into a single result,
  can return a list containing duplicated items.


1.9.1
=====

**PR 129: Fix problematic pytest dependency (C4-521)**

* Fix problem in 1.9.0 with unwanted dependency on
  ``pytest.PytestConfigWarning`` (C4-521).
* Added some unit tests to run instead of integration tests for
  ``s3_utils`` in a number of cases.


1.9.0
=====

**PR 128: Changelog Warnings (C4-511) and Publish Fixes (C4-512)**

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

**PR 127: Beanstalk Bugfix**

* Parses Beanstalk API correctly and passes region.

1.8.3
=====

**No PR: Just fixes to GA PyPi deploy**

1.8.2
=====

**PR 126: C4-503 Grab Environment API**

* Adds get_beanstalk_environment_variables, which will return information 
  necessary to simulate any application given the caller has the appropriate 
  access keys.
* Removes an obsolete tag from create_db_snapshot, which was set erroneously.

1.8.1
=====

**PR 125: Edits to getting_started doc**

* Edited getting_started.rst doc to reflect updated account creation protocol.

1.8.0
=====

**PR 124: Add url_path_join**

* Add ``misc_utils.url_path_join`` for merging parts of URLs.
* Add ``make retest`` to rerun failed tests from previous test run.

1.7.1
=====

**PR 123: Add GA for build**

* Adds 3 Github Actions for building the library, building docs
  and deploying to PyPi

1.7.0
=====

**PR 122: Speed up ff_utils unit tests, and misc small bits of functionality**

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

**PR 121: More time functions**

In ``misc_utils``:

* Fix ``as_datetime`` to raise an error on bad input, allowing `raise_error=False`
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

**PR 120: Update ES-py Version**

* Updates elasticsearch library to 6.8.1 to take a bug fix.


1.5.0
=====

**PR 119: More env_utils support**

* Add ``env_utils.classify_server_url``.


1.4.0
=====

**PR 118: Various bits of functionality in support of 4dn-status (C4-363)**

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

**PR 117: Repair handling of sentry_dsn in deployment_utils (C4-361)**

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

**PR 115: Miscellaneous fixes 2020-10-06**

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

**PR 114: Port some utility**

* New ``ff_utils`` functions
  for common pages/info we'd like to obtain:
  ``get_health_page``, ``get_counts_page``,
  ``get_indexing_status``, and ``get_counts_summary``.
* New ``CachedField`` facility.
* New ``misc_utils`` functions ``camel_case_to_snake_case``,
  ``snake_case_to_camel_case``, and ``make_counter``.


1.2.0
=====

**PR 113: Deprecations, updates + CNAME swap**

* Implements an ``obsolete`` decorator,
  applied to many functions in ``beanstalk_utils``.
* Fixes some functions in ``beanstalk_utils``
  that do not work with ES6
* Pull full ``CNAME`` swap code from ``Torb`` into ``dcicutils``.

**PR 112: Miscellaneous utilities ported from cgap-portal and SubmitCGAP repos**

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

**PR 111: ES6 - Fix create_es_client**

This is a major change, with beta version number 1.0.0.b1:

* Fixes to ``es_utils.create_es_client``.


0.41.0
======

**PR 110: Add VirtualApp.put_json (C4-272)**

* Add ``misc_utils.VirtualApp.put_json``.


Older Versions
==============

A record of older changes can be found
`in GitHub <https://github.com/4dn-dcic/utils/pulls?q=is%3Apr+is%3Aclosed>`_.
To find the specific version numbers, see the ``version`` value in
the ``poetry.app`` section of ``pyproject.toml``, as in::

   [poetry.app]
   name = "dcicutils"
   version = "100.200.300"
   ...etc.

