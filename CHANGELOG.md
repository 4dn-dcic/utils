# Change Log

## 1.3.0

 * Fix a lurking bug in `beanstalk_utils` where `delete_db` had the wrong scope.
 * Add `qa_utils.raises_regexp` for conceptual compatibility with `AssertRaises` in `unittest`.
 * Add `qa_utils.CustomizableProperty` and companion `getattr_customized`.
 * Add `qa_utils.override_dict`, factored out of `qa_utils.override_environ`.
 * Add `qa_utils.check_duplicated_items_by_key` to aid in error reporting for search results.
 * Add support for `sentry_dsn` and a `ENCODED_SENTRY_DSN` 
   beanstalk environment variable in `deployment_utils`.

## 1.2.1

 * New `ff_utils` functions 
   for common pages/info we'd like to obtain:
   `get_health_page`, `get_counts_page`, 
   `get_indexing_status`, and `get_counts_summary`.
 * New `CachedField` facility.
 * New `misc_utils` functions `camel_case_to_snake_case`, 
   `snake_case_to_camel_case`, and `make_counter`.
 
## 1.2.0

 * Implements an `obsolete` decorator, 
   applied to many functions in `beanstalk_utils`.
 * Fixes some functions in `beanstalk_utils`
   that do not work with ES6
 * Pull full `CNAME` swap code from `Torb` into `dcicutils`.
 
 