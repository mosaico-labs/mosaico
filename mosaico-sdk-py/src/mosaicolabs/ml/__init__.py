# ---------------------------------------------------------
# SOFT DEPENDENCY HANDLING
# ---------------------------------------------------------
try:
    from sklearn.base import BaseEstimator, TransformerMixin  # type: ignore
except ImportError:
    # Fallback: Define dummy base classes if scikit-learn is not installed.
    # We implement a basic fit_transform so the SDK's API remains consistent
    # for the user, even without scikit-learn.
    class BaseEstimator:
        pass

    class TransformerMixin:
        def fit_transform(self, X, y=None, **fit_params):
            """
            Fits the transformer to the data and then transforms it.
            """
            # Note: the fit() and transform() classes are implemented by the
            # classes inheriting from this mixin
            if y is None:
                return self.fit(X, **fit_params).transform(X)
            else:
                return self.fit(X, y, **fit_params).transform(X)


# Explicitly export the classes so linters and IDEs know they are public
__all__ = ["BaseEstimator", "TransformerMixin"]

from .data_frame_extractor import DataFrameExtractor as DataFrameExtractor
from .decoding_transformer import VideoDecodingTransformer as VideoDecodingTransformer
from .sync_policies.hold import (
    SyncAsOf as SyncAsOf,
    SyncDrop as SyncDrop,
    SyncHold as SyncHold,
)
from .sync_policy import SyncPolicy as SyncPolicy
from .sync_transformer import SyncTransformer as SyncTransformer
