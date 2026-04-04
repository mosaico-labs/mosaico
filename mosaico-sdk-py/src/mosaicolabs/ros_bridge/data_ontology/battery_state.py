from typing import Optional

from mosaicolabs.models import MosaicoType, Serializable
from mosaicolabs.models.types import MosaicoField


class BatteryState(Serializable):
    """
    Represents the state of a battery power supply.

    modeled after: [sensor_msgs/msg/BatteryState](https://docs.ros2.org/foxy/api/sensor_msgs/msg/BatteryState.html)

    Note:
        This model is still not included in the default ontology of Mosaico and is defined specifically for the ros-bridge module
    """

    # __msco_pyarrow_struct__ = pa.struct(
    #     [
    #         # --- Power Metrics ---
    #         pa.field("voltage", pa.float32(), metadata={"unit": "V"}),
    #         pa.field("temperature", pa.float32(), metadata={"unit": "C"}),
    #         pa.field("current", pa.float32(), metadata={"unit": "A"}),
    #         pa.field("charge", pa.float32(), metadata={"unit": "Ah"}),
    #         pa.field("capacity", pa.float32(), metadata={"unit": "Ah"}),
    #         pa.field("design_capacity", pa.float32(), metadata={"unit": "Ah"}),
    #         pa.field("percentage", pa.float32(), metadata={"range": "0-1"}),
    #         # --- Status & ID ---
    #         # Storing enums as integers to be efficient and language-agnostic
    #         pa.field("power_supply_status", pa.uint8()),
    #         pa.field("power_supply_health", pa.uint8()),
    #         pa.field("power_supply_technology", pa.uint8()),
    #         pa.field("present", pa.bool_()),
    #         pa.field("location", pa.string()),
    #         pa.field("serial_number", pa.string()),
    #         # --- Cell Data ---
    #         # Using lists for variable-length cell data
    #         pa.field("cell_voltage", pa.list_(pa.float32()), nullable=True),
    #         pa.field("cell_temperature", pa.list_(pa.float32()), nullable=True),
    #     ]
    # )

    # Core Metrics
    voltage: MosaicoType.float32 = MosaicoField(description="The battery voltage")
    """The battery voltage value"""

    temperature: Optional[MosaicoType.float32] = MosaicoField(
        default=None, description="The battery temperature in °C"
    )
    """The optional battery temperature in °C"""

    current: Optional[MosaicoType.float32] = MosaicoField(
        default=None, description="Optional battery charge in A."
    )
    """The optional battery current in A"""

    charge: Optional[MosaicoType.float32] = MosaicoField(
        default=None, description="Optional battery charge in Ah."
    )
    """The optional battery charge in Ah"""

    capacity: Optional[MosaicoType.float32] = MosaicoField(
        default=None, description="Optional batterty capacity in Ah."
    )
    """The optional battery capacity in Ah"""

    design_capacity: Optional[MosaicoType.float32] = MosaicoField(
        default=None, description="Optional battery design capacity in Ah."
    )
    """The optional battery design capacity in Ah"""

    percentage: MosaicoType.float32 = MosaicoField(
        description="Battery percentage in %."
    )
    """The battery percentage in %"""

    # Status
    power_supply_status: MosaicoType.uint8 = MosaicoField(
        description="The charging status."
    )
    """The charging status"""

    power_supply_health: MosaicoType.uint8 = MosaicoField(
        description="The battery health."
    )
    """The battery health"""

    power_supply_technology: MosaicoType.uint8 = MosaicoField(
        description="The battery technology."
    )
    """The battery technology"""

    present: MosaicoType.bool = MosaicoField(description="Battery presence.")
    """The battery presence"""

    # Metadata
    location: MosaicoType.string = MosaicoField(
        description="Battery location (like the slot)."
    )
    """The battery location (like the slot)"""

    serial_number: MosaicoType.string = MosaicoField(
        description="Battery serial number."
    )
    """The battery serial number"""

    # Cell Details (Optional because some drivers don't report them)
    cell_voltage: Optional[MosaicoType.list_(MosaicoType.float32)] = MosaicoField(
        default=None, nullable=True, description="Battery cells voltage."
    )
    """The battery cells voltage"""

    cell_temperature: Optional[MosaicoType.list_(MosaicoType.float32)] = MosaicoField(
        default=None, nullable=True, description="Battery cells temperature."
    )
    """The battery cells temperature"""
