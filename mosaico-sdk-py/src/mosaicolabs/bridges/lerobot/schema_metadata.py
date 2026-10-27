"""
The reserved `_lerobot_` metadata namespace attached to the sequences created by the LeRobot
bridge.

LeRobot metadata is keyed by feature names such as `observation.state`, while the Mosaico server
rejects metadata keys containing `.`, and non-finite floats (`NaN`, `inf`), which are not valid
JSON. The namespace therefore stores:

- `info` (`meta/info.json`) with its `features` as a JSON string, which is not queryable;
- `episode` (the `meta/episodes` row of the episode) with its `stats` nested on `.`
  (`observation.state` -> `observation` / `state`) and their non-finite floats replaced by
  `None`, so that the sequences can be queried by the statistics of their episode.

`meta/stats.json` is not stored: on export, it is computed from the statistics of the exported
episodes. `LeRobotSchemaMetadata.restore` reverses these transformations.
"""

import json
import math
from typing import Any, ClassVar, Dict, Optional

from ..base_schema_metadata import BaseSchemaMetadata
from .metadata.helpers import _flatten_stats, _unflatten


def _replace_non_finite(value: Any) -> Any:
    """Replaces the non-finite floats (`NaN`, `inf`) of a JSON-like value, at any depth, by `None`."""
    if isinstance(value, dict):
        return {key: _replace_non_finite(val) for key, val in value.items()}
    if isinstance(value, (list, tuple)):
        return [_replace_non_finite(item) for item in value]
    if isinstance(value, float) and not math.isfinite(value):
        return None
    return value


class LeRobotSchemaMetadata(BaseSchemaMetadata):
    """
    Encapsulates Mosaico's reserved ``_lerobot_`` metadata namespace in a single place.

    It holds, on each sequence created by the LeRobot bridge (one per episode), the content of
    `meta/info.json` (`info`), the `meta/episodes` row of the episode (`episode`) and the name of
    the dataset folder (`source_file`). The topics carry no `_lerobot_` metadata.

    Every field set on this namespace is made server-compatible on the fly (see the module
    docstring), and the resulting keys are validated, so that an invalid key is reported before
    any request reaches the server. `restore` gives the fields back as they were set.

    Example:
        ```python
        meta = LeRobotSchemaMetadata(
            episode={"length": 10, "stats": {"observation.state": {"max": [1.0, 2.0]}}}
        )
        sequence_metadata = meta.merge_into({"operator": "John"})
        # sequence_metadata == {
        #     "operator": "John",
        #     "_lerobot_": {
        #         "episode": {"length": 10, "stats": {"observation": {"state": {"max": [1.0, 2.0]}}}},
        #     },
        # }
        LeRobotSchemaMetadata.restore(sequence_metadata)
        # == {"episode": {"length": 10, "stats": {"observation.state": {"max": [1.0, 2.0]}}}}
        ```
    """

    KEY: ClassVar[str] = "_lerobot_"
    """The reserved metadata key."""

    def __init__(self, **fields: Any):
        super().__init__(**self._sanitize(fields))

    def update(self, **fields: Any) -> "LeRobotSchemaMetadata":
        super().update(**self._sanitize(fields))
        return self

    @classmethod
    def restore(cls, metadata: Optional[dict]) -> Dict[str, Any]:
        """
        Reads the `_lerobot_` block of a sequence metadata, with its fields as they were set: the
        `features` of `info` decoded from their JSON string, and the dotted feature names of the
        episode `stats` rebuilt (their non-finite floats stay `None`).

        Args:
            metadata (Optional[dict]): The sequence metadata (e.g. `SequenceHandler.user_metadata`).

        Returns:
            Dict[str, Any]: The restored block, or an empty dict if `metadata` has none.

        Raises:
            KeyError: If `info` has no `features`.
            ValueError: If a JSON string cannot be decoded.
        """
        block = cls.extract(metadata)
        if "info" in block:
            info = block["info"]
            block["info"] = {**info, "features": json.loads(info["features"])}
        if "stats" in block.get("episode", {}):
            episode = block["episode"]
            block["episode"] = {**episode, "stats": _flatten_stats(episode["stats"])}
        return block

    @staticmethod
    def _sanitize(fields: Dict[str, Any]) -> Dict[str, Any]:
        sanitized = dict(fields)
        if "info" in fields:
            info = fields["info"]
            sanitized["info"] = {**info, "features": json.dumps(info["features"])}
        if "stats" in fields.get("episode", {}):
            episode = fields["episode"]
            stats = _unflatten(_replace_non_finite(episode["stats"]), sep=".")
            sanitized["episode"] = {**episode, "stats": stats}

        return sanitized
