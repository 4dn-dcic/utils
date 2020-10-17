======================
dcicutils
======================

----------
Change Log
----------


1.3.0
=====

**PR 115: Miscellaneous fixes 2020-10-06**

* Fix a lurking bug in ``beanstalk_utils`` where ``delete_db`` had the wrong scope.
* Add ``qa_utils.raises_regexp`` for conceptual compatibility with ``AssertRaises`` in ``unittest``.
* Add ``misc_utils.CustomizableProperty`` and companion ``misc_utils.getattr_customized``.
* Add ``qa_utils.override_dict``, factored out of ``qa_utils.override_environ``.
* Add ``qa_utils.check_duplicated_items_by_key`` to aid in error reporting for search results.
* Add ``qa_utils.MockUUIDModule`` for being able to mock ``uuid.uuid4()``.
* Add support for ``sentry_dsn`` and a ``ENCODED_SENTRY_DSN``
  beanstalk environment variable in ``deployment_utils``.
* Convert test for ``ff_utils.search_metadata`` to be a proper unit test to avoid
  some timing errors that occur during integration testing. I wrote 


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

