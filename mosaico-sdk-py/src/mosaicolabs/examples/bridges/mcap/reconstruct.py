from pathlib import Path

from mosaicolabs.bridges.mcap import MCAPExtractorConfig, MCAPSequenceExtractor
from mosaicolabs.examples.config import (
    API_KEY,
    ASSET_DIR,
    ENABLE_TLS,
    LOG_LEVEL,
    MOSAICO_HOST,
    MOSAICO_PORT,
)

RECONSTRUCTED_MCAP_FILE_PATH = Path(ASSET_DIR) / "reconstructed"

SEQUENCE_NAMES = [
    "trio_connecting_cables_to_adapters_recording_0001",
    "bimanual_xarm6_place_clothing_into_basket_recording_0001",
    "bimanual_xarm6_remove_hats_and_dirty_laundry_recording_0001",
    "bimanual_xarm6_watering_house_plant_recording_0001",
]


def main():

    for sequence in SEQUENCE_NAMES:
        configs = MCAPExtractorConfig(
            saving_path=RECONSTRUCTED_MCAP_FILE_PATH,
            sequence_name=sequence,
            host=MOSAICO_HOST,
            port=MOSAICO_PORT,
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
        extractor = MCAPSequenceExtractor(configs)
        extractor.run()


if __name__ == "__main__":
    # Setup simple logging for background SDK processes
    main()
