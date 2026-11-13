"""
MkDocs hook: sets config.extra.docs_version to the version these docs are built from.

Release docs are built by .github/workflows/docs.yml on a `v*` tag push, where
GitHub sets GITHUB_REF_TYPE=tag and GITHUB_REF_NAME to the tag. Any other build
(e.g. local) is labelled "development" so it can't be mistaken for the official
docs. overrides/partials/source.html shows it in the header.
"""

import os

from mkdocs.config.defaults import MkDocsConfig

DEV_LABEL = "development"


def on_config(config: MkDocsConfig) -> MkDocsConfig:
    tag = None
    if os.environ.get("GITHUB_REF_TYPE") == "tag":
        tag = os.environ.get("GITHUB_REF_NAME")
    config.extra["docs_version"] = tag or DEV_LABEL
    return config
