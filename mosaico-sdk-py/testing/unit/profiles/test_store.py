import datetime
import math
import os
import stat
import sys

import pytest

from mosaicolabs.profiles import (
    Profile,
    ProfileConfigError,
    ProfileExistsError,
    ProfileNotFoundError,
    ProfileStore,
)

if sys.version_info >= (3, 11):
    import tomllib
else:
    import tomli as tomllib


@pytest.fixture
def store(tmp_path):
    return ProfileStore(tmp_path / "mosaico" / "config.toml")


def test_add_get_remove(store):
    store.add(Profile(name="dev", host="http://localhost:1234/"))
    assert store.get("dev") == Profile(name="dev", host="localhost:1234", default=True)

    with pytest.raises(ProfileExistsError):
        store.add(Profile(name="dev", host="other"))
    store.add(Profile(name="dev", host="other"), overwrite=True)
    assert store.get("dev").host == "other"

    store.remove("dev")
    with pytest.raises(ProfileNotFoundError):
        store.get("dev")


def test_default_handling(store):
    assert store.add(Profile(name="a", host="a")).default
    assert not store.add(Profile(name="b", host="b")).default

    store.set_default("b")
    assert store.get_default().name == "b"
    assert store.remove("b").name == "a"
    assert store.get_default().name == "a"


def test_reads_legacy_file_and_escapes_strings(store):
    store.path.parent.mkdir()
    store.path.write_text('[old]\nhost = "h"\nport = "7000"\nextra = "kept"\n')
    assert store.get("old").port == 7000

    store.add(Profile(name="new profile", host="h", api_key='a"b\\c'))
    assert store.get("new profile").api_key == 'a"b\\c'
    assert 'extra = "kept"' in store.path.read_text()


def test_keeps_other_toml_values(store):
    store.path.parent.mkdir()
    store.path.write_text(
        '[old]\nhost = "h"\ncreated = 2024-01-02\ntags = ["a", "b"]\n'
        "low = -inf\nhigh = inf\nmissing = nan\n"
    )
    store.add(Profile(name="new", host="n"))

    old = tomllib.loads(store.path.read_text())["old"]
    assert old["created"] == datetime.date(2024, 1, 2)
    assert old["tags"] == ["a", "b"]
    assert (old["low"], old["high"]) == (-math.inf, math.inf)
    assert math.isnan(old["missing"])


def test_symlinked_config_stays_a_symlink(store, tmp_path):
    target = tmp_path / "dotfiles" / "config.toml"
    target.parent.mkdir()
    target.write_text("")
    store.path.parent.mkdir()
    store.path.symlink_to(target)

    store.add(Profile(name="dev", host="h"))

    assert store.path.is_symlink()
    assert store.get("dev").host == "h"
    assert "[dev]" in target.read_text()


@pytest.mark.parametrize(
    "content",
    [
        "not [[ valid",
        '[a]\nhost = "a"\ndefault = true\n[b]\nhost = "b"\ndefault = true\n',
    ],
    ids=["invalid-toml", "two-defaults"],
)
def test_invalid_file_raises(store, content):
    store.path.parent.mkdir()
    store.path.write_text(content)
    with pytest.raises(ProfileConfigError):
        store.list()


def test_writes_are_private_and_atomic(store, monkeypatch):
    store.add(Profile(name="dev", host="h", api_key="secret"))
    assert stat.S_IMODE(os.stat(store.path).st_mode) == 0o600
    before = store.path.read_text()

    def fail(*args):
        raise OSError("disk full")

    monkeypatch.setattr(os, "replace", fail)
    with pytest.raises(ProfileConfigError):
        store.add(Profile(name="prod", host="p"))
    assert store.path.read_text() == before
    assert os.listdir(store.path.parent) == ["config.toml"]
