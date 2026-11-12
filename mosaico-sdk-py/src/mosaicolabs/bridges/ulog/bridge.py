from typing import Dict, Optional, Type

from .adapter_base import ULogAdapterBase


class ULogBridge:
    """
    Registry of the hand-written ULog adapters, keyed by ULog format name
    (e.g. `sensor_combined`). Topics whose format has no adapter here are stored as
    `Unmodeled` (see `UnmodeledAdapter`).

    Same pattern as `MCAPBridge` (bridges/mcap/bridge.py), keyed by format name instead
    of schema name + encoding (ULog has a single encoding). Empty for now: typed
    adapters (e.g. `sensor_combined` -> IMU) can be added later without touching the
    loader or the injector.
    """

    _default_adapters: Dict[str, Type[ULogAdapterBase]] = {}

    @classmethod
    def get_default_adapter(cls, format_name: str) -> Optional[Type[ULogAdapterBase]]:
        """Returns the adapter registered for `format_name`, or `None`."""
        return cls._default_adapters.get(format_name)


def register_default_adapter(format_name: str):
    """
    Class decorator registering a ULog adapter for the format `format_name`.

    Same as the MCAP `register_default_adapter` (duplicates are rejected); the format
    name is passed to the decorator instead of being read from class attributes.

    Example:
        ```python
        @register_default_adapter("sensor_combined")
        class SensorCombinedAdapter(ULogAdapterBase[IMU]): ...
        ```

    Raises:
        ValueError: If an adapter is already registered for `format_name`.
    """

    def wrap(cls: Type[ULogAdapterBase]) -> Type[ULogAdapterBase]:
        if format_name in ULogBridge._default_adapters:
            raise ValueError(
                f"Adapter for ULog format '{format_name}' is already registered."
            )
        ULogBridge._default_adapters[format_name] = cls
        return cls

    return wrap
