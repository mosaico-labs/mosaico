from typing import List, Optional
import pyarrow as pa
from mosaicolabs.models import Serializable, HeaderMixin


class BatteryState(Serializable, HeaderMixin):
    """
    Represents the state of a battery power supply.

    modeled after: [sensor_msgs/msg/BatteryState](https://docs.ros2.org/foxy/api/sensor_msgs/msg/BatteryState.html)

    Note:
        This model is still not included in the default ontology of Mosaico and is defined specifically for the ros-bridge module
    """

    __msco_pyarrow_struct__ = pa.struct(
        [
            # --- Power Metrics ---
            pa.field("voltage", pa.float32(), metadata={"unit": "V"}),
            pa.field("temperature", pa.float32(), metadata={"unit": "C"}),
            pa.field("current", pa.float32(), metadata={"unit": "A"}),
            pa.field("charge", pa.float32(), metadata={"unit": "Ah"}),
            pa.field("capacity", pa.float32(), metadata={"unit": "Ah"}),
            pa.field("design_capacity", pa.float32(), metadata={"unit": "Ah"}),
            pa.field("percentage", pa.float32(), metadata={"range": "0-1"}),
            # --- Status & ID ---
            # Storing enums as integers to be efficient and language-agnostic
            pa.field("power_supply_status", pa.uint8()),
            pa.field("power_supply_health", pa.uint8()),
            pa.field("power_supply_technology", pa.uint8()),
            pa.field("present", pa.bool_()),
            pa.field("location", pa.string()),
            pa.field("serial_number", pa.string()),
            # --- Cell Data ---
            # Using lists for variable-length cell data
            pa.field("cell_voltage", pa.list_(pa.float32()), nullable=True),
            pa.field("cell_temperature", pa.list_(pa.float32()), nullable=True),
        ]
    )

    # Core Metrics
    voltage: float
    """The battery voltage value"""
    temperature: Optional[float]
    """The optional battery temperature in °C"""
    current: Optional[float]
    """The optional battery current in A"""
    charge: Optional[float]
    """The optional battery charge in Ah"""
    capacity: Optional[float]
    """The optional battery capacity in Ah"""
    design_capacity: Optional[float]
    """The optional battery design capacity in Ah"""
    percentage: float
    """The battery percentage in %"""

    # Status
    power_supply_status: int
    """The charging status"""
    power_supply_health: int
    """The battery health"""
    power_supply_technology: int
    """The battery technology"""
    present: bool
    """The battery presence"""

    # Metadata
    location: str
    """The battery location (like the slot)"""
    serial_number: str
    """The battery serial number"""

    # Cell Details (Optional because some drivers don't report them)
    cell_voltage: Optional[List[float]] = None
    """The battery cells voltage"""
    cell_temperature: Optional[List[float]] = None
    """The battery cells temperature"""
