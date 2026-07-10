# Project agent memory

This file is the project's committed home for project-intrinsic agent knowledge: build, test, release, architecture, and sharp-edge notes that should travel with the code.

- Add durable project-specific notes here as they are discovered through real work.

## Automatic tag-and-publish-on-master release workflow

`.github/workflows/main.yml`'s `publish` job (`needs: build`, runs only on `push` to
`master`) reads the version from `pyproject.toml` (`poetry version -s`) and checks both for
its git tag and for an existing PyPI release. It creates the tag if missing and publishes
only if PyPI does not already have the version, all **in the same job run**. It deliberately
does not rely on `.github/workflows/main-publish.yml`'s tag-triggered `on: push: tags`
event, because GitHub Actions does not start a new workflow run from a tag pushed using the
default `GITHUB_TOKEN` (anti-recursion rule) - `main-publish.yml` remains only for manual/
`workflow_dispatch` publishing.

The "Create and push tag" step is gated on the tag-existence check (`exists == 'false'`) -
tag once per version. The "Publish to PyPI" step instead uses the independently checked
`pypi_exists == 'false'` condition. Its curl request treats only HTTP 200 (present) and 404
(absent) as valid; any other status fails the job closed. This split preserves recovery from
a tag-exists-but-never-published state, while avoiding a failing duplicate-upload attempt on
later merges. `dcicutils/scripts/publish_to_pypi.py` itself also independently verifies the
version isn't already published and refuses a dirty checkout, as defense-in-depth.

Unlike snovault (which vendors this same job), this repo IS `dcicutils`, so `make
publish-for-ga` invokes `python -m dcicutils.scripts.publish_to_pypi --noconfirm` directly
against the checked-out source - no separate "install dcicutils" step is needed. The
`publish` job's "Install Deps" step mirrors `main-publish.yml`'s: `make configure` (installs
poetry) plus `pip install requests toml` (the modules `publish_to_pypi.py` imports outside
of poetry's venv). The workflow asserts `git diff --exit-code` right after that install step
so a future dependency-install change that dirties the tree fails loud with the filename,
even though no known step currently does so here.

The workflow-level `permissions:` block in `main.yml` stays `id-token: write` / `contents:
read` (needed by the `build` job's AWS OIDC auth); `contents: write` (needed to push the
tag) is scoped to the `publish` job only, since a job-level `permissions:` block replaces
rather than merges with the workflow-level one.

## Maintaining this file

Keep this file for knowledge useful to almost every future agent session in this project.
Do not repeat what the codebase already shows; point to the authoritative file or command instead.
Prefer rewriting or pruning existing entries over appending new ones.
When updating this file, preserve this bar for all agents and keep entries concise.
