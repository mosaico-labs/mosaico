from typing import Any, ClassVar, Dict, Optional, Type


class BaseSchemaMetadata:
    """
    Encapsulates Mosaico's reserved topic-metadata namespace in a single place.

    Every topic ingested by the MCAP or ROS bridge carries specific bookkeeping:
     - ``channel_name``, ``channel_encoding``, ``schema_name``, ``schema_encoding``, raw ``schema_def`` for MCAP
     - ``msgtype``, raw ``msgdef``, extracted ``enums`` for ROS

    plus bridge-internal fields (e.g. the source mcap file) under one reserved key, so that:

    * The literal ``KEY`` string exists in exactly one place, instead of being duplicated across adapters,
      loaders, and the injector.
    * Callers build up this namespace incrementally via `update` without ever touching
      the wrapping dict shape by hand.
    """

    KEY: ClassVar[str] = ""
    """The reserved metadata key. It should be overridden by each BaseSchemaMetadata specification."""

    REQUIRED_KEYS: ClassVar[Dict[str, Type]] = {}
    """The keys that need to be present in order to create the schema metadata. Empty by default"""

    def __init__(self, **fields: Any):

        # Check that REQUIRED_KEYS are present
        missing_required_keys = set(self.REQUIRED_KEYS.keys()) - set(fields.keys())

        if missing_required_keys:
            raise ValueError(
                "Some among the required keys are missing. "
                f"Missing keys are: {[k for k in missing_required_keys]}"
            )

        # Check that existing keys have expected type
        for key, exp_type in self.REQUIRED_KEYS.items():
            value = fields[key]

            if not isinstance(value, exp_type):
                raise TypeError(
                    f"Required key {key} within metadata but it has unexpected type. "
                    f"Expected {exp_type.__name__} but got {type(value).__name__}"
                )

        self.fields: dict = dict(fields)

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)

        # Check that KEY exists and is not empty
        if not cls.KEY:
            raise TypeError(f"{cls.__name__} must defined a non-empty KEY.")

    def update(self, **fields: Any) -> "BaseSchemaMetadata":
        """
        Merges additional fields into this block, in place. Returns `self` for chaining.

        Args:
            **fields (Any): Additional fields to merge.

        Returns:
            BaseSchemaMetadata: The updated metadata instance.
        """
        self.fields.update(fields)
        return self

    def to_dict(self) -> dict:
        """
        Wraps the current fields under the reserved key.

        Returns:
            dict: A dictionary containing the `KEY` block with the current fields.
        """
        return {self.KEY: dict(self.fields)}

    def merge_into(self, metadata: dict) -> dict:
        """
        Merges this block into an existing metadata dict's `KEY` namespace, creating it
        if absent. Mutates and returns `metadata`.

        Args:
            metadata (dict): The existing metadata dict to merge into.

        Returns:
            dict: The updated metadata dict with the `KEY` block merged in.
        """
        metadata.setdefault(self.KEY, {}).update(self.fields)
        return metadata

    @classmethod
    def extract(cls, metadata: Optional[dict]) -> dict:
        """
        Reads the `KEY` block out of a metadata dict or `{}` if absent.

        Args:
            metadata (Optional[dict]): A metadata dict, typically `{KEY: {...}}` (e.g. `{"_ros_": {...}}` or `{"_mcap_": {...}}`), or `None`.

        Returns:
            dict: The extracted `KEY` block, or an empty dict if not present.
        """
        return dict((metadata or {}).get(cls.KEY) or {})

    @classmethod
    def from_dict(cls, metadata: Optional[dict]) -> "BaseSchemaMetadata":
        """
        Creates a `BaseSchemaMetadata` from a plain metadata dict, e.g. the return value of
        `ROSAdapterBase.schema_metadata()`. Any keys outside the `KEY` namespace are ignored.

        Args:
            metadata (Optional[dict]): A metadata dict, typically `{KEY: {...}}` or `None`.

        Returns:
            BaseSchemaMetadata: A new instance seeded with the extracted `KEY` fields
                (empty if `metadata` is `None` or carries no `KEY` block, and `REQUIRED_KEYS`
                is empty).

        Raises:
            ValueError: If any of `REQUIRED_KEYS` is missing from the extracted fields.
            TypeError: If any of `REQUIRED_KEYS` has an unexpected type.
        """
        return cls(**cls.extract(metadata))
