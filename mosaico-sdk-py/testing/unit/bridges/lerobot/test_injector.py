from unittest.mock import MagicMock

from mosaicolabs.bridges.lerobot.injector import (
    LeRobotInjectionConfig,
    LeRobotInjector,
)
from mosaicolabs.bridges.lerobot.schema_metadata import LeRobotSchemaMetadata

from .config import CAMERA_COLUMN_NAMES, STATE_COLUMN_NAME

MAX_BATCH_SIZE_BYTES = 64 * 1024 * 1024


def test_injection_drops_the_filtered_columns_from_the_metadata(lerobot_dataset):
    """A column excluded by `column_names` leaves no trace in the `_lerobot_` metadata of the
    sequence: neither in `info.features` nor in the episode `stats`"""
    excluded = CAMERA_COLUMN_NAMES[1]  # observation.images.wrist
    injector = LeRobotInjector(
        LeRobotInjectionConfig(file_path=lerobot_dataset, column_names=[f"!{excluded}"])
    )
    episode = next(iter(injector._open_or_get_loader(MAX_BATCH_SIZE_BYTES)))
    # Records the `sequence_create` call and absorbs the upload
    mclient = MagicMock()

    injector._upload_episode(mclient, episode, MagicMock())

    metadata = mclient.sequence_create.call_args.kwargs["metadata"]
    block = LeRobotSchemaMetadata.restore(metadata)
    assert excluded not in block["info"]["features"]
    assert excluded not in block["episode"]["stats"]
    # The accepted columns are still described
    assert STATE_COLUMN_NAME in block["info"]["features"]
    assert STATE_COLUMN_NAME in block["episode"]["stats"]
