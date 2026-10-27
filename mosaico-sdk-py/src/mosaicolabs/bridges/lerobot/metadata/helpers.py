"""
Helpers of the LeRobot metadata: the nesting of flat keys (e.g. `data/chunk_index` ->
`data` / `chunk_index`), its reverse, and its reverse for the statistics of the features.
"""

from typing import Any, Dict


def _merge_nested(dst: Dict[str, Any], src: Dict[str, Any], path: str) -> None:
    """Recursively merges `src` into `dst`, raising if a key is both a value and a nested object."""
    for key, value in src.items():
        key_path = f"{path}.{key}" if path else key
        if key not in dst:
            dst[key] = value
        elif isinstance(dst[key], dict) and isinstance(value, dict):
            _merge_nested(dst[key], value, key_path)
        else:
            raise ValueError(
                f"Metadata key conflict on '{key_path}': it is defined both as a value "
                "and as a nested object."
            )


def _unflatten(flat: Dict[str, Any], sep: str) -> Dict[str, Any]:
    """Turns `{"a/b": 1, "a/c": 2}` into `{"a": {"b": 1, "c": 2}}` (with `sep="/"`)."""
    out: Dict[str, Any] = {}
    for key, value in flat.items():
        *parents, leaf = key.split(sep)
        nested: Dict[str, Any] = {leaf: value}
        for parent in reversed(parents):
            nested = {parent: nested}
        _merge_nested(out, nested, "")
    return out


def _flatten(nested: Dict[str, Any], sep: str, prefix: str = "") -> Dict[str, Any]:
    """Turns `{"a": {"b": 1, "c": 2}}` into `{"a/b": 1, "a/c": 2}` (with `sep="/"`): the
    reverse of `_unflatten`."""
    flat: Dict[str, Any] = {}
    for key, value in nested.items():
        path = f"{prefix}{sep}{key}" if prefix else key
        if isinstance(value, dict):
            flat.update(_flatten(value, sep, path))
        else:
            flat[path] = value
    return flat


def _flatten_stats(nested: Dict[str, Any], prefix: str = "") -> Dict[str, Any]:
    """
    Reverses `_unflatten(stats, sep=".")` on per-feature statistics: turns
    `{"observation": {"state": {"mean": [...]}}}` into `{"observation.state": {"mean": [...]}}`.

    At each level, the values that are not dicts are the statistics of the feature named by the
    path to that level (joined with `.`), and the dicts are deeper levels of feature names. This
    relies on a statistic never being a dict (LeRobot stores lists or numbers), and also restores
    a feature whose name is the prefix of another one (e.g. `a` and `a.b`). A feature with no
    statistics is lost.
    """
    stats = {key: value for key, value in nested.items() if not isinstance(value, dict)}
    flat: Dict[str, Any] = {prefix: stats} if stats else {}
    for key, value in nested.items():
        if isinstance(value, dict):
            flat.update(_flatten_stats(value, f"{prefix}.{key}" if prefix else key))
    return flat
