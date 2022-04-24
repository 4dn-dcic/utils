=========
dcicutils
=========

----------
Change Log
----------


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
  but only for legacy environments. This is not a good solution for orchestrated environments (C4-689).

* The variables ``FF_MAGIC_CNAME``, ``CGAP_MAGIC_CNAME``, ``FF_GOLDEN_DB``, and ``CGAP_GOLDEN_DB``,
  which had no uses outside of ``dcicutils`` itself,
  now have underscores ahead of their names to emphasize that they are internal to ``dcicutils`` only.
  ``_FF_MAGIC_CNAME``, ``_CGAP_MAGIC_CNAME``, ``_FF_GOLDEN_DB``, and ``_CGAP_GOLDEN_DB``, respectively,
  could be used as a direct replacement in an emergency,
  but only for legacy environments. This is not a good solution for orchestrated environments (C4-689).

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

* In ``s3_utils``, fix C4-706, where short names of environments were not accepted
  as env arguments to s3Utils in legacy CGAP.


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

**PR 150: Add json_leaf_subst, conjoined_list and disjoined_list**

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

**PR 148: Support auth0 client and secret in deployment_utils**

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

**PR 147: Init s3Utils via GLOBAL_ENV_BUCKET and misc S3_BUCKET_ORG support (C4-554)**
**PR 146: Better S3 bucket management in deployment_utils**

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

**PR 145: Fix internal import problems**

* Make ``lang_utils`` import ``ignored`` from ``misc_utils``, not ``qa_utils``.
* Make ``deployment_utils`` import ``override_environ`` from ``misc_utils``, not ``qa_utils``.
* Move ``local_attrs`` from ``qa_utils`` to ``misc_utils``
  so that similar errors can be avoided in other libraries that import it.


1.18.0
======

**PR 141: Port Application Dockerization utils**

* Add additional ECS related APIs needed for orchestration/deployment.


1.17.0
======

**PR 144: Add known_bug_expected and related support**

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

**PR 142: Move override_environ and override_dict to misc_utils**

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

**PR 138: JH Docker Mount Update**

* In ``jh_utils.find_valid_file_or_extra_file``,
  account for file metadata containing an
  ``"open_data_url"``.


1.15.0
======

**PR 140: Add misc_utils.is_valid_absolute_uri (C4-651)**

* Adds ``misc_utils.is_valid_absolute_uri``
  for RFC 3986 compliance.


1.14.1
======

**PR 139: Add ES cluster resize capability**

* Adds ElasticSearchServiceClient, a wrapper for boto3.client('es')
* Implements resize_elasticsearch_cluster, issuing an update to the relevant settings
* Integrated test was performed on staging
* Unit tests mock the boto3 API


1.14.0
======

**PR 137: Docker, ECR, ECS Utils**

* Adds 3 new modules with basic functionality needed for further development on the alpha stack
* Deprecates Python 3.4


1.13.0
======

**PR 136: Support for VirtualApp.post**

* Add a ``post`` method to ``VirtualApp`` for situations where ``post_json``
  is not appropriate.



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

