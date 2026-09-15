from pathlib import Path

from rosbags.rosbag2 import StoragePlugin
from rosbags.typesys import Stores

from mosaicolabs.bridges.ros import ROSExtractorConfig, ROSSequenceExtractor
from mosaicolabs.examples.config import (
    API_KEY,
    ASSET_DIR,
    ENABLE_TLS,
    LOG_LEVEL,
    MOSAICO_HOST,
    MOSAICO_PORT,
)

BAG_FILE_PATH = Path(ASSET_DIR) / "reconstructed"

SEQUENCE_NAMES = [
    "r2b_galileo2_0",
    "r2b_galileo_0",
    "r2b_whitetunnel_0",
    "r2b_robotarm_0",
]


ROS_DISTRO = Stores.ROS2_JAZZY


def main():

    for sequence in SEQUENCE_NAMES:
        configs = ROSExtractorConfig(
            rosbag_path=BAG_FILE_PATH,
            sequence_name=sequence,
            host=MOSAICO_HOST,
            port=MOSAICO_PORT,
            ros_distro=ROS_DISTRO,
            storage_plugin=StoragePlugin.MCAP,
            # custom_msgs=,
            # topics=["*odom*"],
            log_level=LOG_LEVEL,
            mosaico_api_key=API_KEY,
            # tls_cert_path=,
            enable_tls=ENABLE_TLS,
            # start_timestamp_ns=,
            # end_timestamp_ns=,
            overwrite=True,
        )

        # --- Execution ---
        extractor = ROSSequenceExtractor(configs)
        extractor.run()


if __name__ == "__main__":
    # Setup simple logging for background SDK processes
    main()
