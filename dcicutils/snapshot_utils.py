import datetime
import logging

from elasticsearch.exceptions import NotFoundError
from .misc_utils import (
    environ_bool, PRINT, camel_case_to_snake_case, full_object_name,
    ignorable, ancestor_classes, decorator, ignored,
)


class _ElasticSearchDataCache:
    """ Caches whether or not we have already provisioned a particular body of data. """

    # Invoking tests with DEBUG_SNAPSHOTS=TRUE will cause pdb breakpoints in certain situations.
    DEBUG_SNAPSHOTS = environ_bool("DEBUG_SNAPSHOTS", default=False)

    # Invoking tests with VERBOSE_SNAPSHOTS=TRUE will cause debugging typeout to appear.
    VERBOSE_SNAPSHOTS = environ_bool("VERBOSE_SNAPSHOTS", default=DEBUG_SNAPSHOTS)

    # Invoking tests with ENABLE_SNAPSHOTS=TRUE will cause this facility to work.
    # It is off by default because right now it only works on local machines.
    # TODO: Provision our servers to have a storage area to which snapshots can be made.
    ENABLE_SNAPSHOTS = environ_bool("ENABLE_SNAPSHOTS", default=False)

    _REGISTERED_DATA_CACHES = set()
    _ABSTRACT_DATA_CACHES = set()
    _DATA_CACHE_BASE_CLASS = None

    _SNAPSHOTS_INITIALIZED = {}

    repository_short_name = 'snapshots'
    snapshots_repository_location = None
    indexer_namespace = None

    @classmethod
    def assure_data_once_loaded(cls, es_testapp, datadir, indexer_namespace,
                                snapshot_name=None, other_data=None, level=0):
        """
        Initialize the data associated with a a snapshot if it has not already been done.

        If initialization had already been done, this does NOT repeat it. Of course, it's possible
        that this was done and then the environment was later changed, for example in another test,
        and is not precisely what it was. Some fixtures work this way for legacy or efficiency reasons.

        :param es_testapp: an es_testapp fixture (providing an endpoint with admin access to test application with ES)
        :param datadir: the name of the temporary directory allocated for use of this test run
        :param indexer_namespace: the prefix string to be used on all ES index names
        :param snapshot_name: an optional snapshot name to override the default for special uses.
            The default value of None asks it be inferred from information declared as the snapshot_name class variable.
        :param other_data: a parameter passed through to the load_additional_data method if loading is needed.
            The nature of this data, if provided, depends on the class. The default value is None.
            (This can be useful if the load_additional_data method needs to receive fixture values.)
        :param level: This is used internally and should not be passed explicitly.  It helps with indentation
            and is used as a prefix when DEBUG_WORKBOOK_CACHE is True and is otherwise ignored.
        """
        if cls.VERBOSE_SNAPSHOTS:
            if level == 0:
                PRINT()
            PRINT(level * "  ", level,
                  "Entering %s.assure_data_once_loaded at %s" % (cls.__name__, datetime.datetime.now()))
        cls.assure_data_loaded(es_testapp,
                               # Just pass these arguments through
                               datadir=datadir, indexer_namespace=indexer_namespace, snapshot_name=snapshot_name,
                               other_data=other_data,
                               level=level + 1,
                               # This is the important part, supporting the caller's promise that once initialized,
                               # state will remain sufficiently consistent that reinitialization can be skipped.
                               only_on_first_call=True)
        if cls.VERBOSE_SNAPSHOTS:
            PRINT(level * "  ", level,
                  "Exiting %s.assure_data_once_loaded at %s" % (cls.__name__, datetime.datetime.now()))

    @classmethod
    def assure_data_loaded(cls, es_testapp, datadir, indexer_namespace,
                           snapshot_name=None, other_data=None, level=0,
                           only_on_first_call=False):
        """
        Creates (and remembers) or else restores the ES data associated with this class.

        :param es_testapp: an es_testapp fixture (providing an endpoint with admin access to test application with ES)
        :param datadir: the name of the temporary directory allocated for use of this test run.
            All filenames used by this facility for snapshots are computed relative to the datadir.
        :param indexer_namespace: the prefix string to be used on all ES index names
        :param snapshot_name: an optional snapshot name to override the default for special uses.
            The default value of None asks it be inferred from information declared as the snapshot_name class variable.
        :param other_data: a parameter passed through to the load_additional_data method if loading is needed.
            The nature of this data, if provided, depends on the class. The default value is None.
            (This can be useful if the load_additional_data method needs to receive fixture values.)
        :param only_on_first_call: (deprecated, default False)
            If True, restoration of data is suppressed after its initial creation.
            If False, if the data is not newly created, it is restored from a snapshot.
        :param level: This is used internally and should not be passed explicitly.  It helps with indentation
            and is used as a prefix when DEBUG_WORKBOOK_CACHE is True and is otherwise ignored.
        """
        if not cls.ENABLE_SNAPSHOTS:
            only_on_first_call = True

        if cls.VERBOSE_SNAPSHOTS:
            if level == 0:
                PRINT()
            PRINT(level * "  ", level, "Entering %s.assure_data_loaded at %s" % (cls.__name__, datetime.datetime.now()))

        snapshot_name = cls.defaulted_snapshot_name(snapshot_name)
        cls._setattrs_safely(snapshots_repository_location=cls.make_snapshot_location(datadir),
                             indexer_namespace=indexer_namespace)
        if not cls.is_snapshot_initialized(snapshot_name):
            cls.load_data(es_testapp, datadir=datadir, indexer_namespace=indexer_namespace, other_data=other_data,
                          level=level + 1)
            if cls.VERBOSE_SNAPSHOTS:
                PRINT(level * "  ", level, "Creating snapshot", snapshot_name, "at", datetime.datetime.now())
            cls.create_snapshot(es_testapp,
                                snapshots_repository_location=cls.snapshots_repository_location,
                                repository_short_name=cls.repository_short_name,
                                indexer_namespace=indexer_namespace,
                                snapshot_name=snapshot_name)
            cls.mark_snapshot_initialized(snapshot_name)
            if cls.VERBOSE_SNAPSHOTS:
                PRINT(level * "  ", level, "Done creating snapshot", snapshot_name, "at", datetime.datetime.now())
        elif only_on_first_call:
            # This supports an optimization in which a fixture is run only once when setting up and not again
            # rather than reinitializing on every attempt. That only works if the tests of the data are sufficiently
            # stable, or tests are sufficiently robust, that everything can still pass.
            if cls.VERBOSE_SNAPSHOTS:
                PRINT(level * "  ", level, "Skipping snapshot restoration because only_first_on_call=True.")
            pass
        else:
            if cls.VERBOSE_SNAPSHOTS:
                PRINT(level * "  ", level, "Restoring snapshot", snapshot_name, "at", datetime.datetime.now())
            cls.restore_snapshot(es_testapp,
                                 indexer_namespace=cls.indexer_namespace,
                                 repository_short_name=cls.repository_short_name,
                                 snapshot_name=snapshot_name)
            if cls.VERBOSE_SNAPSHOTS:
                PRINT(level * "  ", level, "Done restoring snapshot", snapshot_name, "at", datetime.datetime.now())

        if cls.VERBOSE_SNAPSHOTS:
            PRINT(level * "  ", level, "Exiting %s.assure_data_loaded at %s" % (cls.__name__, datetime.datetime.now()))

    @classmethod
    def load_data(cls, es_testapp, datadir, indexer_namespace, other_data=None, level=0):
        if cls.VERBOSE_SNAPSHOTS:
            PRINT(level * "  ", level, "Entering %s.load_data at %s" % (cls.__name__, datetime.datetime.now()))
        if not cls.is_data_cache(cls):
            raise RuntimeError("The class %s is not a registered data cache class."
                               " It may need an @%s.register() decoration."
                               % (cls.__name__, full_object_name(cls._DATA_CACHE_BASE_CLASS)))
        if cls.VERBOSE_SNAPSHOTS:
            PRINT(level * "  ", level, "Checking ancestors of", cls.__name__)
        ancestor_found = None
        for ancestor_class in ancestor_classes(cls):
            if cls.VERBOSE_SNAPSHOTS:
                PRINT(level * "  ", level, "Trying ancestor", ancestor_class)
            # We only care about classes that are descended from our root class, obeying our protocols,
            # and actually allowed to have snapshots made (i.e., not declared abstract). Other mixed in
            # classes can be safely ignored.
            if cls.is_data_cache(ancestor_class):
                if ancestor_found:
                    if not issubclass(ancestor_found, ancestor_class):
                        # This could happen with multiple inheritance. We can't rely on just calling its
                        # assure_data_loaded method because that method will blow away all indexes to build
                        # its foundation and we've already done that.  Even if we worked backward and loaded
                        # the less specific type first, risking reloads, that would only work for single
                        # inheritance, since it would again blow away the foundation before loading another layer,
                        # so we require single-inheritance and just assume the top layer knows what it's doing.
                        # -kmp 14-Feb-2021
                        raise RuntimeError("%s requires its descendants to use only single inheritance"
                                           ", but %s mixes %s and %s, and %s is not a subclass of %s."
                                           % (cls._DATA_CACHE_BASE_CLASS.__name__,
                                              cls.__name__,
                                              ancestor_found.__name__,
                                              ancestor_class.__name__,
                                              ancestor_found.__name__,
                                              ancestor_class.__name__))
                else:
                    ancestor_found = ancestor_class
        if ancestor_found:
            if cls.VERBOSE_SNAPSHOTS:
                PRINT(level * "  ", level, "Assuring data for ancestor class", ancestor_found.__name__,
                      "on behalf of", cls.__name__)
            ancestor_found.assure_data_loaded(es_testapp,
                                              datadir=datadir,
                                              indexer_namespace=indexer_namespace,
                                              other_data=other_data, level=level+1)
            if cls.VERBOSE_SNAPSHOTS:
                PRINT(level * "  ", level, "Done assuring data for ancestor class", ancestor_found.__name__,
                      "on behalf of", cls.__name__)
        else:
            if cls.VERBOSE_SNAPSHOTS:
                PRINT(level * "  ", level, "No useful ancestor found. No foundation to load.", cls.__name__)
        # Having built a foundation, now add the data that we wanted.
        if cls.VERBOSE_SNAPSHOTS:
            PRINT(level * "  ", level, "Loading additional requested class data", cls.__name__)
        # Now that a proper foundation is assured, load the new data that this class contributes.
        cls.load_additional_data(es_testapp, other_data=other_data)
        if cls.VERBOSE_SNAPSHOTS:
            PRINT(level * "  ", level, "Done loading additional requested class data", cls.__name__)
        # Finally, assure everything is indexed.
        if cls.VERBOSE_SNAPSHOTS:
            PRINT(level * "  ", level, "Starting indexing at", datetime.datetime.now())
        es_testapp.post_json('/index', {'record': False})
        if cls.VERBOSE_SNAPSHOTS:
            PRINT(level * "  ", level, "Done indexing at", datetime.datetime.now())
        if cls.VERBOSE_SNAPSHOTS:
            PRINT(level * "  ", level, "Exiting %s.load_data at %s" % (cls.__name__, datetime.datetime.now()))

    @classmethod
    def load_additional_data(cls, es_testapp, other_data=None):
        """
        The default method does no setup, so a snapshot will not be interesting,
        but this is a useful base case so that anyone writing a subclass can customize
        this method by doing:

        class MyData(ElasticSearchDataCache):

            @classmethod
            def load_additional_data(cls, es_testapp, other_data=None):
                # This should NOT call super(). That will be done in other ways.
                ... load data into environment belonging to parent ...
        """
        pass

    @classmethod
    def defaulted_snapshot_name(cls, snapshot_name):
        return snapshot_name or camel_case_to_snake_case(cls.__name__) + "_snapshot"

    @classmethod
    def make_snapshot_location(cls, datadir):
        return datadir + "/" + cls.repository_short_name

    @classmethod
    def mark_snapshot_initialized(cls, snapshot_name):
        cls._SNAPSHOTS_INITIALIZED[snapshot_name] = True

    @classmethod
    def is_snapshot_initialized(cls, snapshot_name):
        return cls._SNAPSHOTS_INITIALIZED.get(snapshot_name)

    @classmethod
    def _setattrs_safely(cls, **attributes_and_values):
        """Sets various class variables making sure they're not already set incompatibly."""
        for attr, value in attributes_and_values.items():
            existing = getattr(cls, attr)
            if existing and existing != value:
                if cls.DEBUG_SNAPSHOTS:
                    import pdb
                    pdb.set_trace()  # noQA
                raise RuntimeError("Conflicting %s: %s (new) and %s (existing)." % (attr, value, existing))
            setattr(cls, attr, value)

    @classmethod
    def is_valid_indexer_namespace(cls, indexer_namespace, allow_null=False):
        """
        Returns true if its argument is a valid indexer namespace.

        For non-shared resources, the null string is an allowed namespace.
        For all shared resources, a string ending in a digit is required.

        This test allows these formats:

        kind                      examples (not necessarily exhaustive)
        ----                      -------------------------------------
        travis or github job id   ...-test-...  (typically including a Travis or GitHub Actions job id)
        any guid                  c978d7ab-e970-417b-8cb1-4516546e6ced
        a timestamp               20211202123456, 2021-12-02T12:34:56, or 2021-12-02T12:34:56.0000
        any above with a prefix   4dn-20211202123456, sno-20211202123456, cgap-20211202123456, ff-20211202123456
        """
        if indexer_namespace == "":
            # Allow the empty string only in production situations
            return True if allow_null else False
        if not isinstance(indexer_namespace, str):
            # Non-strings (including None) are not indexer_namespaces
            return False
        # At this point we know we have a non-empty string, so check that last 4 characters match of one of our options.
        return bool(indexer_namespace)  # Maybe: '-test-' in indexer_namespace or len(indexer_namespace) >= 4

    @classmethod
    def snapshot_exists(cls, es_testapp, repository_short_name, snapshot_name):
        es = es_testapp.app.registry['elasticsearch']
        try:
            return bool(es.snapshot.get(repository=repository_short_name, snapshot=snapshot_name))
        except NotFoundError:
            return False

    @classmethod
    def repository_spec(cls, snapshots_repository_location, repository_short_name, indexer_namespace, snapshot_name):
        # If this method was customized in different ways in a subclass, these other arguments might be useful.
        ignored(repository_short_name, indexer_namespace, snapshot_name)
        return {
            "type": "fs",
            "settings": {
                "location": snapshots_repository_location,
            }
        }

    @classmethod
    def create_snapshot(cls, es_testapp, snapshots_repository_location, repository_short_name,
                        indexer_namespace, snapshot_name):
        if not cls.ENABLE_SNAPSHOTS:
            if cls.VERBOSE_SNAPSHOTS:
                PRINT("NOT creating snapshot because %s.ENABLE_SNAPSHOTS is False." % full_object_name(cls))
            return
        if cls.snapshot_exists(es_testapp, repository_short_name, snapshot_name):
            return
        es = es_testapp.app.registry['elasticsearch']
        try:

            if cls.VERBOSE_SNAPSHOTS:
                PRINT("Creating snapshot repo", repository_short_name, "at", snapshots_repository_location)
            spec = cls.repository_spec(snapshots_repository_location=snapshots_repository_location,
                                       repository_short_name=repository_short_name,
                                       indexer_namespace=indexer_namespace, snapshot_name=snapshot_name)
            repo_creation_result = es.snapshot.create_repository(repository_short_name, spec)
            assert repo_creation_result == {'acknowledged': True}
            if cls.VERBOSE_SNAPSHOTS:
                PRINT("Creating snapshot", repository_short_name)
            ignored(indexer_namespace)  # It would be nice to makea  snapshot only of part of the data
            snapshot_creation_result = es.snapshot.create(repository=repository_short_name,
                                                          snapshot=snapshot_name,
                                                          wait_for_completion=True)
            assert snapshot_creation_result.get('snapshot', {}).get('snapshot') == snapshot_name
        except Exception as e:
            logging.error(str(e))
            if cls.DEBUG_SNAPSHOTS:
                import pdb
                pdb.set_trace()  # noQA
            raise

    @classmethod
    def restore_snapshot(cls, es_testapp, indexer_namespace, repository_short_name, snapshot_name=None,
                         require_indexer_namespace=True):
        if not cls.ENABLE_SNAPSHOTS:
            if cls.VERBOSE_SNAPSHOTS:
                PRINT("NOT restoring snapshot because %s.ENABLE_SNAPSHOTS is False." % full_object_name(cls))
            return
        es = es_testapp.app.registry['elasticsearch']
        try:
            if require_indexer_namespace:
                if not indexer_namespace or not cls.is_valid_indexer_namespace(indexer_namespace):
                    raise RuntimeError("restore_snapshot requires an indexer namespace prefix (got %r)."
                                       " (You can use the indexer_namespace fixture to acquire it.)"
                                       % (indexer_namespace,))

            all_index_info = [info['index'] for info in es.cat.indices(format='json')]
            index_names = [name for name in all_index_info if name.startswith(indexer_namespace)]
            if index_names:
                index_names_string = ",".join(index_names)
                if cls.VERBOSE_SNAPSHOTS:
                    # PRINT("Deleting index files", index_names_string)
                    PRINT("Deleting index files for prefix=", indexer_namespace)
                result = es.indices.delete(index_names_string)
                if cls.VERBOSE_SNAPSHOTS:
                    ignorable(result)
                    # PRINT("deletion result=", result)
                    PRINT("Deleted index files for prefix=", indexer_namespace)
            result = es.snapshot.restore(repository=repository_short_name,
                                         snapshot=snapshot_name,
                                         wait_for_completion=True)
            # Need to find out what a successful result looks like
            if cls.VERBOSE_SNAPSHOTS:
                ignorable(result)
                # PRINT("restore result=", result)
                PRINT("restored snapshot_name=", snapshot_name)
        except Exception as e:
            # Maybe should log somehow?
            logging.error(str(e))
            # Maybe should reset cls.done to False?
            if cls.DEBUG_SNAPSHOTS:
                import pdb
                pdb.set_trace()  # noQA
            raise

    @classmethod
    def register(cls, is_abstract=False, _is_base=False):

        def _wrap_registered(class_being_declared):

            if _is_base:
                if cls._DATA_CACHE_BASE_CLASS:
                    raise RuntimeError("Attempt to declare %s with _base=True, but %s has already been declared."
                                       % (class_being_declared.__name__, full_object_name(cls._DATA_CACHE_BASE_CLASS)))
                cls._DATA_CACHE_BASE_CLASS = class_being_declared
            elif not cls._DATA_CACHE_BASE_CLASS:
                raise RuntimeError("Attempt to use @data_cache decorator for the first time on %s, but is_base=%s."
                                   % (class_being_declared.__name__, _is_base))

            if not issubclass(class_being_declared, cls._DATA_CACHE_BASE_CLASS):
                raise SyntaxError("The data_cache class %s does not inherit, directly or indirectly, from %s."
                                  % (class_being_declared.__name__, full_object_name(cls._DATA_CACHE_BASE_CLASS)))

            cls._REGISTERED_DATA_CACHES.add(class_being_declared)

            if is_abstract:
                cls._ABSTRACT_DATA_CACHES.add(class_being_declared)

            return class_being_declared

        return _wrap_registered

    @classmethod
    def is_data_cache(cls, candidate_class, allow_abstract=False):
        return (candidate_class in cls._REGISTERED_DATA_CACHES
                and (allow_abstract or cls._is_abstract_data_cache(candidate_class)))

    @classmethod
    def _is_abstract_data_cache(cls, candidate_class):
        return candidate_class not in cls._ABSTRACT_DATA_CACHES


@decorator()
def es_data_cache(is_abstract=False, _is_base=False):
    return _ElasticSearchDataCache.register(is_abstract=is_abstract, _is_base=_is_base)


@es_data_cache(is_abstract=True, _is_base=True)
class ElasticSearchDataCache(_ElasticSearchDataCache):
    pass
