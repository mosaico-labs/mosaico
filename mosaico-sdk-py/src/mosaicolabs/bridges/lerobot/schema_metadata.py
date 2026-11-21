"""
The reserved `_lerobot_` metadata namespace attached to the sequences created by the LeRobot
bridge.

LeRobot metadata is keyed by feature names such as `observation.state`, while the Mosaico server
rejects metadata keys containing `.`: the namespace therefore nests dotted keys
(`observation.state` -> `observation` / `state`) and drops non-finite floats (rejected by the
server JSON parser), so that its content is always accepted by the server.
"""

import math
from typing import Any, ClassVar, Dict

from mosaicolabs.handlers.helpers import _validate_metadata_keys

from ..base_schema_metadata import BaseSchemaMetadata
from .metadata.helpers import _unflatten


def _to_mosaico_metadata(value: Any) -> Any:
    """
    Makes a JSON-like value storable as Mosaico metadata.

    Keys containing `.` are nested (`{"observation.state": x}` -> `{"observation": {"state": x}}`),
    and non-finite floats (`NaN`, `inf`), which are not valid JSON, are replaced by `None`.
    """
    if isinstance(value, dict):
        return _unflatten(
            {str(key): _to_mosaico_metadata(val) for key, val in value.items()},
            sep=".",
        )
    if isinstance(value, (list, tuple)):
        return [_to_mosaico_metadata(item) for item in value]
    if isinstance(value, float) and not math.isfinite(value):
        return None
    return value


class LeRobotSchemaMetadata(BaseSchemaMetadata):
    """
    Encapsulates Mosaico's reserved ``_lerobot_`` metadata namespace in a single place.

    It holds, on each sequence created by the LeRobot bridge (one per episode), the content of
    `meta/info.json` (`info`) and `meta/stats.json` (`stats`), the `meta/episodes` row of the
    episode (`episode`) and the name of the dataset folder (`source_file`). The topics carry no
    `_lerobot_` metadata.

    Every field set on this namespace is made server-compatible on the fly: keys containing
    `.` (e.g. the feature name `observation.state`) are nested, non-finite floats are replaced
    by `None`, and the resulting keys are validated, so that an invalid key is reported before
    any request reaches the server.

    Example:
        ```python
        meta = LeRobotSchemaMetadata(info={"features": {"observation.state": {"dtype": "float32"}}})
        sequence_metadata = meta.merge_into({"operator": "John"})
        # sequence_metadata == {
        #     "operator": "John",
        #     "_lerobot_": {"info": {"features": {"observation": {"state": {"dtype": "float32"}}}}},
        # }
        ```
    """

    KEY: ClassVar[str] = "_lerobot_"
    """The reserved metadata key."""

    def __init__(self, **fields: Any):
        super().__init__(**self._sanitize(fields))

    def update(self, **fields: Any) -> "LeRobotSchemaMetadata":
        super().update(**self._sanitize(fields))
        return self

    @staticmethod
    def _sanitize(fields: Dict[str, Any]) -> Dict[str, Any]:
        sanitized = _to_mosaico_metadata(fields)
        _validate_metadata_keys(sanitized)
        return sanitized
