# Releasing

Release Please reads `release-please-config.json`, tracks the last published
version in `.release-please-manifest.json`, and updates `VERSION` in its release
pull request.

The `RELEASE_PLEASE_TOKEN` repository secret is required. It MUST be a
fine-grained personal access token or GitHub App installation token with
permission to create release pull requests, tags, and releases. The workflow
does not use `GITHUB_TOKEN`, because events created by that token do not trigger
the downstream tag-based artifact workflow.

Pre-1.0 feature and breaking changes advance the minor version. There is no
persistent `release-as` override: after `0.5.0`, the same configuration can
calculate `0.6.0` rather than remaining pinned to the prior release.
