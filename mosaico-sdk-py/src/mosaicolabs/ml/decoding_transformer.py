from typing import List

import pandas as pd

from mosaicolabs.logging_config import get_logger
from mosaicolabs.models.sensors.image import (
    ImageFormat,
    StatefulDecodingSession,
    _StatelessDefaultCodec,
)

# Import the base classes from the parent __init__.py
from . import BaseEstimator, TransformerMixin

# Set the hierarchical logger
logger = get_logger(__name__)


class VideoDecodingTransformer(BaseEstimator, TransformerMixin):
    """
    A Scikit-Learn compatible stateful transformer that reconstructs CompressedImage
    byte streams into usable PIL Images chronologically before temporal synchronization.

    This transformer bridges the gap between video compression mechanics (which rely on
    inter-frame dependencies like I/P/B frames) and machine learning batching strategies.
    It maintains a persistent decoding session across data chunks, ensuring that
    delta-frames are correctly decoded using the proper reference frames before any
    temporal synchronization or randomization occurs downstream.

    Attributes:
        topics (List[str]): A list of Mosaico topic names (e.g., "/front/camera/image")
            that contain the compressed image data to be decoded.
        _stateless_codec: The codec used for stateless formats like JPEG or PNG.
        _stateful_decoding_session_type: The class/factory used to instantiate the
            stateful session for formats like H.264 or HEVC.
        _session: The active decoding session instance.
    """

    def __init__(
        self,
        topics: List[str],
        stateless_codec=_StatelessDefaultCodec(),
        stateful_decoding_session_type=StatefulDecodingSession,
    ):
        """
        Initializes the VideoDecodingTransformer.

        Args:
            topics (List[str]): The list of topics to target for image decoding.
            stateless_codec (optional): An instance of a codec to handle formats that
                do not require state (e.g., JPEG). Defaults to _StatelessDefaultCodec().
            stateful_decoding_session_type (optional): The class to instantiate for
                state-dependent video decoding. Defaults to StatefulDecodingSession.
        """
        self._topics = topics
        self._stateful_decoding_session_type = stateful_decoding_session_type
        self._session = None
        self._stateless_codec = stateless_codec

    def fit(self, X: pd.DataFrame, y=None):
        """
        Initializes the persistent decoding session.

        This method should be called before transforming the first chunk of data.
        It establishes the stateful session required to track reference frames
        across subsequent `transform` calls.

        Note: Architectural Note
            The C-level decoding session is intentionally initialized here rather than in
            `__init__()`. This complies with Scikit-Learn's strict "no side-effects"
            contract for constructors (allowing safe `sklearn.base.clone` operations),
            ensures the transformer can be pickled for multiprocessing (e.g., via `joblib`),
            and enforces lazy resource allocation.

            The session is initialized at the first function call. Any other method call
            does nothing, unless the [`reset()`][mosaicolabs.ml.VideoDecodingTransformer.reset]
            method is called.

        Args:
            X (pd.DataFrame): The input DataFrame chunk (unused in this method but
                required by the Scikit-Learn API).
            y (optional): Target values (unused).

        Returns:
            self: Returns the transformer instance.
        """
        # Initialize the persistent decoding session on the first chunk
        if self._session is None:
            self._session = self._stateful_decoding_session_type()
        return self

    def transform(self, X: pd.DataFrame) -> pd.DataFrame:
        """
        Decodes the compressed image data within the DataFrame chronologically.

        For each requested topic, this method extracts the raw bytes and image format.
        Stateful formats (H.264, HEVC) are routed through the persistent decoding session,
        using the topic name as the context to isolate decoder states per camera.
        Stateless formats (JPEG) are routed to the stateless codec.

        The resulting `PIL.Image` objects are inserted into a new column named
        `{topic}.compressed_image.decoded`, and the original raw byte/format columns are dropped
        to conserve memory.

        Example:
            ```python
            # Obtain a dataframe with DataFrameExtractor
            from mosaicolabs import MosaicoClient, IMU, Image
            from mosaicolabs.ml import DataFrameExtractor, VideoDecodingTransformer

            with MosaicoClient.connect("localhost", 6726) as client:
                sequence_handler = client.sequence_handler("example_sequence")
                # Resample at 30Hz and fill the NaNs with a `Hold` policy
                vdec_transf = VideoDecodingTransformer(
                    topics=["/front_stereo_camera/left/image_compressed"]
                ) # (1)!

                for df in DataFrameExtractor(sequence_handler).to_pandas_chunks():
                    decoded_df = vdec_transf.fit_transform(df) # (2)!
                    # Do something with the decoded dataframe
                    # ...
            ```

            1. Note: the `VideoDecodingTransformer` is created outside the chunk-related `for` loop: the transformer is
                a **stateful state-machine** designed to maintain signal continuity across different data chunks.
            2. The decoded image is here: `decoded_df["/front_stereo_camera/left/image_compressed.compressed_image.decoded"]`

        Args:
            X (pd.DataFrame): A chronologically ordered sparse chunk from the `DataFrameExtractor`,
                with a new column named `{topic}.compressed_image.decoded`.

        Returns:
            pd.DataFrame: A new DataFrame containing the fully reconstructed `PIL.Image`
                objects in place of the raw byte streams.
        """
        if X.empty:
            return X

        if self._session is None:
            raise ValueError(
                "`VideoDecodingTransformer._session` not initialized. "
                "Call `VideoDecodingTransformer.fit()` or "
                "`VideoDecodingTransformer.fit_transform()`."
            )

        X_out = X.copy()

        for topic in self._topics:
            # Mosaico flat column convention: {topic_name}.{ontology_tag}.{field}
            data_col = f"{topic}.compressed_image.data"
            fmt_col = f"{topic}.compressed_image.format"

            if data_col not in X.columns or fmt_col not in X.columns:
                continue

            decoded_col = f"{topic}.compressed_image.decoded"
            decoded_images = []

            # Iterate strictly chronologically
            for _, row in X.iterrows():
                img_bytes = row[data_col]
                fmt_val = row[fmt_col]

                if pd.isna(img_bytes) or not img_bytes:
                    decoded_images.append(None)
                    continue

                img_format = (
                    ImageFormat(fmt_val) if isinstance(fmt_val, str) else fmt_val
                )

                # Stateful decoding for video streams
                if img_format in [ImageFormat.H264, ImageFormat.HEVC]:
                    pil_img = self._session.decode(
                        img_bytes=img_bytes,
                        format=img_format,
                        context=topic,  # Isolates decoder state per topic
                    )
                # Stateless fallback for JPEGs/PNGs
                else:
                    pil_img = self._stateless_codec.decode(
                        img_bytes=img_bytes, format=img_format
                    )

                if pil_img is None:
                    logger.warning("Unable to decode the image. Storing as 'None'")

                decoded_images.append(pil_img)

            # Insert the fully reconstructed images back into the dataframe
            X_out[decoded_col] = decoded_images

            # Clean up raw byte columns to save RAM before passing to the ML model
            X_out.drop(columns=[data_col, fmt_col], inplace=True)

        return X_out

    def reset(self):
        """
        Releases the C-level decoder resources and resets the session state.

        This should be called when processing is complete or if the sequence is
        restarted, to prevent memory leaks and ensure a clean state for the next run.
        """
        if self._session:
            self._session.close()
            self._session = None
