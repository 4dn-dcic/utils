# Project agent memory

This is the committed guide to durable, project-intrinsic knowledge. Prefer the
authoritative files named below over copying details into this document.

## Repository map and public boundaries

- `dcicutils/` is one flat, pip-installable utility package; `pyproject.toml` is the
  authority for supported Python versions, dependencies, package metadata, and console
  entry points. There is intentionally little exported from `dcicutils/__init__.py`:
  consumers import the utility modules directly, so module-level names can be public API.
- General-purpose, dependency-light helpers live in modules such as `misc_utils.py`,
  `lang_utils.py`, `datetime_utils.py`, `file_utils.py`, `data_utils.py`, and
  `schema_utils.py`. Portal-facing APIs are primarily `ff_utils.py` (request/auth and
  metadata functions), `portal_utils.py` (the higher-level `Portal` wrapper),
  `portal_object_utils.py`, `structured_data.py`, and `submitr/`.
- Integration modules are grouped by the system named in the file: AWS (`s3_utils.py`,
  `ecs_utils.py`, `ecr_utils.py`, `cloudformation_utils.py`, `secrets_utils.py`, etc.),
  search (`es_utils.py`, `opensearch_utils.py`), Redis, Docker, and deployment utilities.
  Keep network, credentials, and service assumptions at those boundaries; use
  `ff_mocks.py` and test fixtures rather than live services in unit tests.
- `dcicutils/scripts/` backs the installed commands declared in `pyproject.toml`.
  `docs/source/` is the Sphinx documentation source; `license_policies/` and
  `kibana/` are packaged data/configuration. Top-level `scripts/` contains repository
  maintenance helpers, not installed library API.

## Configuration and compatibility

- `env_utils.py` is the supported environment-name/configuration facade. It dispatches
  between orchestrated configuration and compatibility behavior in
  `env_utils_legacy.py`; `env_base.py`, `env_manager.py`, and `common.py` define the
  underlying configuration vocabulary. Do not call the internal `EnvUtils` class as a
  consumer API or casually remove legacy dispatch: downstream Fourfront/CGAP users rely
  on both modes. The orchestrated and legacy compatibility suites under `test/` are the
  executable contract.
- Environment-backed integrations may require AWS credentials, `GLOBAL_ENV_BUCKET`, or
  `S3_ENCRYPT_KEY`; see `.github/workflows/main.yml`, `docs/source/getting_started.rst`,
  and the relevant module tests. Tests and static CI use `NO_SERVER_FIXTURES` and
  `USE_SAMPLE_ENVUTILS` where appropriate to avoid accidental server configuration.
- Preserve backward-compatible imports and signatures unless a breaking release is
  explicitly intended. Search for deprecation/legacy comments and corresponding tests
  before moving symbols. In particular, portal authentication accepts several historical
  forms, environment naming spans Beanstalk and orchestrated deployments, and
  `qa_utils.py` retains compatibility exports whose supported home is `qa_checkers.py`.
- Elasticsearch is deliberately pinned in `pyproject.toml` for portal compatibility.
  Treat changes to Elasticsearch/OpenSearch, Pyramid, boto, Redis, and urllib/request
  dependencies as integration changes, not routine upgrades.

## Development and tests

- Bootstrap with `make build` (it installs the repository-pinned Poetry and runs
  `poetry install`). Run one focused test with `poetry run pytest test/test_<module>.py`
  or a node id while iterating.
- `Makefile` is the command authority: `make test-units` is the normal local suite,
  `make test-static` runs static-marker tests plus flake8, `make test-integrations`
  selects integration markers, and `make test-for-ga` mirrors the main CI QA command.
  `make test-all` deliberately includes tests likely to fail in ordinary environments;
  `direct_es_query` requires VPC access. Redis-backed tests expect `redis-server` at the
  path configured in `pyproject.toml`.
- Tests mirror modules under `test/test_*.py`; shared helpers/fixtures are in
  `test/helpers.py` and `test/fixtures/`, service recordings in `test/recordings/`, and
  large structured-data samples in `test/data_files/`. Pytest markers and exclusions are
  defined in `pyproject.toml`. Add regression tests beside the affected module and avoid
  refreshing recordings unless the external contract intentionally changed.
- `.github/workflows/main.yml` tests Python 3.11 and 3.12 with Redis and AWS OIDC;
  `.github/workflows/static-checks.yml` runs the static/lint lane. The declared package
  compatibility range is broader than the CI matrix, so avoid syntax or APIs outside
  the range in `pyproject.toml`. Note that the convenience `make lint` target reports
  failures without failing the command; use `make test-static` or direct
  `poetry run flake8 ...` for a gating check.

## Release workflow

- The version in `pyproject.toml` is the release source of truth. After CI succeeds on a
  push to `master`, `.github/workflows/main.yml` creates the matching tag if absent and
  publishes only if that version is absent from PyPI. Those are independent checks so a
  tag-exists-but-unpublished run can recover; unexpected PyPI HTTP responses fail closed.
- Tagging and publishing must remain in that same `publish` job: a tag pushed with the
  default `GITHUB_TOKEN` does not trigger another workflow. Consequently
  `.github/workflows/main-publish.yml` is only a manual/fallback publishing path.
  `dcicutils/scripts/publish_to_pypi.py` also rejects dirty trees and duplicate versions.
- This repository invokes its checkout directly through `make publish-for-ga`; unlike
  repositories that vendor this workflow, it must not install a separate `dcicutils`.
  Keep tag write permission scoped to the publish job because job-level GitHub Actions
  permissions replace, rather than merge with, workflow-level AWS OIDC permissions.

## Maintaining this file

Keep this file for knowledge useful to almost every future agent session in this project.
Do not repeat what the codebase already shows; point to the authoritative file or command instead.
Prefer rewriting or pruning existing entries over appending new ones.
When updating this file, preserve this bar for all agents and keep entries concise.
